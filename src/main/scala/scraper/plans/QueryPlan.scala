package scraper.plans

import scraper.expressions.{ Attribute, Expression }
import scraper.trees.TreeNode

trait QueryPlan[Plan <: TreeNode[Plan]] extends TreeNode[Plan] { self: Plan =>
  def output: Seq[Attribute]

  def references: Seq[Attribute] = expressions.flatMap(_.references)

  def expressions: Seq[Expression] = productIterator.flatMap {
    case element: Expression => Seq(element)
    case element: Seq[_]     => element.collect { case e: Expression => e }
    case _                   => Nil
  }.toSeq

  def withExpressions(newExpressions: Seq[Expression]): Plan = this

  private def copyWithNewExpressionsIfNecessary(newExpressions: Seq[Expression]): Plan = {
    if (this sameExpressions newExpressions) this
    else withExpressions(newExpressions)
  }

  private def sameExpressions(newExpressions: Seq[Expression]): Boolean =
    expressions.length == newExpressions.length &&
      (expressions, newExpressions).zipped.forall(_ sameOrEqual _)

  def transformExpressionsDown(pf: PartialFunction[Expression, Expression]): Plan =
    copyWithNewExpressionsIfNecessary(expressions map (_ transformDown pf))

  def transformExpressionsUp(pf: PartialFunction[Expression, Expression]): Plan =
    copyWithNewExpressionsIfNecessary(expressions map (_ transformUp pf))
}
