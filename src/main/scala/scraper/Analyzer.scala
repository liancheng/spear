package scraper

import scala.util.Success

import scraper.expressions.UnresolvedAttribute
import scraper.plans.logical.{Limit, LogicalPlan, UnresolvedRelation}
import scraper.trees.{Rule, RulesExecutor}

class Analyzer(catalog: Catalog) extends RulesExecutor[LogicalPlan] {
  override def batches: Seq[RuleBatch] = Seq(
    RuleBatch("Resolution", FixedPoint.Unlimited, Seq(
      ResolveRelations,
      ResolveReferences
    )),

    RuleBatch("Type checking", FixedPoint.Unlimited, Seq(
      ImplicitCasts
    ))
  )

  override def apply(tree: LogicalPlan): LogicalPlan = {
    logTrace(
      s"""Analyzing logical query plan:
         |
         |${tree.prettyTree}
       """.stripMargin
    )
    super.apply(tree)
  }

  object ResolveReferences extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case plan if !plan.resolved =>
        plan transformExpressionsUp {
          case UnresolvedAttribute(name) =>
            val candidates = plan.children flatMap (_.output) filter (_.name == name)
            candidates match {
              case Seq(attribute) => attribute
              case Nil            => throw ResolutionFailure(s"Cannot resolve attribute $name")
              case _              => throw ResolutionFailure(s"Ambiguous attribute $name")
            }
        }
    }
  }

  object ResolveRelations extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case UnresolvedRelation(name) => catalog lookupRelation name
    }
  }

  object FoldableLimit extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan @ (_ Limit limit) if limit.foldable => plan
      case plan: Limit =>
        throw new RuntimeException("Limit must be a constant")
    }
  }

  object ImplicitCasts extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case plan =>
        plan transformExpressionsUp {
          case e => e.strictlyTyped.get
        }
    }
  }
}
