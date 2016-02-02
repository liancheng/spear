package scraper.plans.logical

import scraper.Catalog
import scraper.exceptions.{AnalysisException, ResolutionFailureException}
import scraper.expressions._
import scraper.plans.logical.Analyzer._
import scraper.plans.logical.patterns._
import scraper.trees.RulesExecutor.FixedPoint
import scraper.trees.{Rule, RulesExecutor}

class Analyzer(catalog: Catalog) extends RulesExecutor[LogicalPlan] {
  override def batches: Seq[RuleBatch] = Seq(
    RuleBatch("Resolution", FixedPoint.Unlimited, Seq(
      ExpandStars,
      new ResolveRelations(catalog),
      ResolveReferences,
      ResolveSelfJoins
    )),

    RuleBatch("Type check", FixedPoint.Unlimited, Seq(
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
}

object Analyzer {
  /**
   * This rule expands "`*`" appearing in `SELECT`.
   */
  object ExpandStars extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case Unresolved(Resolved(child) Project projectList) =>
        child select (projectList flatMap {
          case Star => child.output
          case e    => Seq(e)
        })
    }
  }

  /**
   * This rule tries to resolve [[scraper.expressions.UnresolvedAttribute UnresolvedAttribute]]s in
   * an logical plan operator using output [[scraper.expressions.Attribute Attribute]]s of its
   * children.
   */
  @throws[ResolutionFailureException](
    "If no candidate or multiple ambiguous candidate input attributes can be found"
  )
  object ResolveReferences extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case Unresolved(plan) if plan.childrenResolved =>
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
                  val list = candidates map (_.debugString) mkString ", "
                  s"Multiple ambiguous input attributes found: $list"
                }
            }
        }
    }
  }

  /**
   * This rule resolves unresolved relations by looking up the table name from the `catalog`.
   */
  class ResolveRelations(catalog: Catalog) extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case UnresolvedRelation(name) => catalog lookupRelation name
    }
  }

  /**
   * This rule tries to transform all resolved logical plans operators (and expressions within them)
   * into strictly typed form by applying implicit casts when necessary.
   */
  @throws[AnalysisException]("If some resolved logical query plan operator doesn't type check")
  object ApplyImplicitCasts extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case Resolved(plan) => plan.strictlyTypedForm.get
    }
  }

  /**
   * This rule eliminates all [[scraper.plans.logical.Subquery Subquery]] operators, since they are
   * only used to provide scoping information during analysis phase.
   */
  object EliminateSubqueries extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Subquery _ => plan
    }
  }

  /**
   * This rule resolves ambiguous attributes introduced by self-joins.
   */
  object ResolveSelfJoins extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case join @ Join(Resolved(left), Resolved(right), _, _) if !join.selfJoinResolved =>
        val conflictingAttributes = left.outputSet & right.outputSet

        def selfJoinInvolved(attributes: Set[Attribute]): Boolean =
          (attributes & conflictingAttributes).nonEmpty

        val newLeft = left.collectFirst {
          // Handles relations that introduce ambiguous attributes. E.g.:
          //
          //   val df = table("t")
          //   val joined = df join df
          //
          // Now both branches of `joined` have exactly the same set of attributes.
          case plan: MultiInstanceRelation if selfJoinInvolved(plan.outputSet) =>
            plan -> plan.newInstance()

          // Handles projections that introduce ambiguous aliases. E.g.:
          //
          //   val projected = df select ('f1 as 'a)
          //   val joined = projected join projected
          //
          // Now `joined` has two alias `a` with the same expression ID in both branches.
          case plan @ Project(_, projectList) if selfJoinInvolved(collectAliases(projectList)) =>
            plan -> plan.copy(projectList = newAliases(projectList))
        } map {
          case (oldPlan, newPlan) =>
            val attributeRewrites = (oldPlan.output zip newPlan.output).toMap

            left transformDown {
              case plan if plan == oldPlan => newPlan
            } transformAllExpressions {
              case a: Attribute => attributeRewrites.getOrElse(a, a)
            }
        } getOrElse left

        join.copy(left = newLeft)
    }

    def collectAliases(projectList: Seq[NamedExpression]): Set[Attribute] =
      projectList.collect { case a: Alias => a.toAttribute }.toSet

    def newAliases(projectList: Seq[NamedExpression]): Seq[NamedExpression] = projectList map {
      case Alias(name, child, _) => child as name
      case e                     => e
    }
  }
}
