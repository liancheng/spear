package scraper.expressions

import scraper.expressions.dsl._
import scraper.trees.{Rule, RulesExecutor}
import scraper.trees.RulesExecutor.FixedPoint

object Predicate {
  private[scraper] def splitConjunction(predicate: Expression): Seq[Expression] = predicate match {
    case left && right => splitConjunction(left) ++ splitConjunction(right)
    case _             => predicate :: Nil
  }

  private[scraper] def toCNF(predicate: Expression): Expression = CNFConverter(predicate)

  private object CNFConverter extends RulesExecutor[Expression] {
    override def batches: Seq[RuleBatch] =
      RuleBatch("CNFConversion", FixedPoint.Unlimited, CNFConversion :: Nil) :: Nil
  }

  private object CNFConversion extends Rule[Expression] {
    override def apply(tree: Expression): Expression = tree transformDown {
      case !(x || y)          => !x && !y
      case !(x && y)          => !x || !y
      case (x && y) || z      => (x || z) && (y || z)
      case x || (y && z)      => (x || y) && (x || z)
      case x && y if x same y => x
    }
  }
}
