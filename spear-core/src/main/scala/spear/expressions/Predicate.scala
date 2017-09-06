package spear.expressions

import spear.trees.{FixedPoint, Rule, RuleGroup, Transformer}

object Predicate {
  private[spear] def splitConjunction(predicate: Expression): Seq[Expression] = predicate match {
    case left && right => splitConjunction(left) ++ splitConjunction(right)
    case _             => predicate :: Nil
  }

  private[spear] def toCNF(predicate: Expression): Expression = cnfConverter(predicate)

  private val cnfConverter = new Transformer(RuleGroup(FixedPoint, CNFConversion :: Nil))

  private object CNFConversion extends Rule[Expression] {
    override def transform(tree: Expression): Expression = tree transformDown {
      case !(x || y)          => !x && !y
      case !(x && y)          => !x || !y
      case (x && y) || z      => (x || z) && (y || z)
      case x || (y && z)      => (x || y) && (x || z)
      case x && y if x same y => x
    }
  }
}
