package scraper

import scraper.expressions.UnresolvedAttribute
import scraper.plans.logical.{ UnresolvedRelation, LogicalPlan }
import scraper.trees.{ Rule, RulesExecutor }

class Analyzer(catalog: Catalog) extends RulesExecutor[LogicalPlan] {
  override def batches: Seq[Batch] = Seq(
    Batch("Resolution", FixedPoint.Unlimited, Seq(
      ResolveRelations,
      ResolveReferences
    )),

    Batch("Type checking", FixedPoint.Unlimited, Seq(
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
    override def apply(tree: LogicalPlan): LogicalPlan = tree.transformUp {
      case plan if !plan.resolved =>
        plan.transformExpressionsUp {
          case UnresolvedAttribute(name) =>
            val candidates = plan.children.flatten(_.output).filter(_.name == name)
            candidates match {
              case Seq(attribute) => attribute
              case Nil            => throw ResolutionFailure(s"Cannot resolve attribute $name")
              case _              => throw ResolutionFailure(s"Ambiguous attribute $name")
            }
        }
    }
  }

  object ResolveRelations extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree.transformUp {
      case UnresolvedRelation(name) => catalog.lookupRelation(name)
    }
  }

  object ImplicitCasts extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree.transformUp {
      case plan =>
        plan.transformExpressionsUp {
          case e if e.typeChecked => e.implicitlyCasted
          case e                  => throw TypeCheckError(e)
        }
    }
  }
}
