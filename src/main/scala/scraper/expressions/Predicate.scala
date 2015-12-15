package scraper.expressions

import scraper.trees.RulesExecutor.FixedPoint
import scraper.trees.{Rule, RulesExecutor}

object Predicate {
  private[scraper] def splitConjunction(predicate: Expression): Seq[Expression] = predicate match {
    case left And right => splitConjunction(left) ++ splitConjunction(right)
    case _              => predicate :: Nil
  }

  private[scraper] def splitDisjunction(predicate: Expression): Seq[Expression] = predicate match {
    case left Or right => splitDisjunction(left) ++ splitDisjunction(right)
    case _             => predicate :: Nil
  }

  private[scraper] def toCNF(predicate: Expression): Expression = CNFConverter(predicate)

  private object CNFConverter extends RulesExecutor[Expression] {
    override def batches: Seq[RuleBatch] =
      RuleBatch("CNFConversion", FixedPoint.Unlimited, CNFConversion :: Nil) :: Nil
  }

  private object CNFConversion extends Rule[Expression] {
    override def apply(tree: Expression): Expression = tree transformDown {
      case Not(Not(x))    => x
      case Not(x Or y)    => !x && !y
      case Not(x And y)   => !x || !y
      case (x And y) Or z => (x || z) && (y || z)
      case x Or (y And z) => (y || x) && (z || x)
    }
  }
}
