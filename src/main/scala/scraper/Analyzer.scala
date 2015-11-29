package scraper

import scraper.expressions.{Star, UnresolvedAttribute}
import scraper.plans.logical.patterns._
import scraper.plans.logical.{LogicalPlan, Project, UnresolvedRelation}
import scraper.trees.{Rule, RulesExecutor}

class Analyzer(catalog: Catalog) extends RulesExecutor[LogicalPlan] {
  override def batches: Seq[RuleBatch] = Seq(
    RuleBatch("Resolution", FixedPoint.Unlimited, Seq(
      ExpandStars,
      ResolveRelations,
      ResolveReferences
    )),

    RuleBatch("Type checking", FixedPoint.Unlimited, Seq(
      ApplyImplicitCasts
    ))
  )

  override def apply(tree: LogicalPlan): LogicalPlan = {
    logTrace(
      s"""Analyzing logical query plan:
         |
         |${tree.prettyTree}
         |""".stripMargin
    )
    super.apply(tree)
  }

  object ExpandStars extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case Unresolved(Resolved(child) Project projections) =>
        child select (projections flatMap {
          case Star => child.output
          case e    => Seq(e)
        })
    }
  }

  object ResolveReferences extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case Unresolved(plan) =>
        plan transformExpressionsUp {
          case UnresolvedAttribute(name) =>
            def reportResolutionFailure(message: String): Nothing = {
              throw new ResolutionFailureException(
                s"""Failed to resolve attribute $name in logical query plan:
                   |
                   |${plan.prettyTree}
                   |
                   |$message
                   |""".stripMargin
              )
            }

            val candidates = plan.children flatMap (_.output) filter (_.name == name)

            candidates match {
              case Seq(attribute) =>
                attribute

              case Nil =>
                reportResolutionFailure("No candidate input attribute(s) found")

              case _ =>
                reportResolutionFailure {
                  val list = candidates map (_.annotatedString) mkString ", "
                  s"Multiple ambiguous input attributes found: $list"
                }
            }
        }
    }
  }

  object ResolveRelations extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case UnresolvedRelation(name) => catalog lookupRelation name
    }
  }

  object ApplyImplicitCasts extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan => plan.strictlyTyped.get
    }
  }
}
