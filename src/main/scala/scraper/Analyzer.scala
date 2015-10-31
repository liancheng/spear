package scraper

import scraper.expressions.UnresolvedAttribute
import scraper.plans.logical.LogicalPlan
import scraper.trees.{ Rule, RulesExecutor }

class Analyzer extends RulesExecutor[LogicalPlan] {
  override def batches: Seq[Batch] = Seq(
    Batch(
      "Resolution",
      Seq(ResolveReferences),
      Once
    )
  )

  object ResolveReferences extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree.transformExpressionsUp {
      case UnresolvedAttribute(name) =>
        val candidates = tree.children.flatMap(_.output).filter(_.name == name)
        candidates match {
          case Seq(attribute) => attribute
          case Nil            => throw ResolutionFailure(s"Cannot resolve attribute $name")
          case _              => throw ResolutionFailure(s"Ambiguous attribute $name")
        }
    }
  }
}
