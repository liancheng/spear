package scraper.expressions

import scraper.expressions.dsl.LogicalOperatorDSL
import scraper.types.{BooleanType, DataType}

trait Predicate extends Expression with LogicalOperatorDSL {
  override def dataType: DataType = BooleanType
}

object Predicate {
  private[scraper] def splitConjunction(predicate: Predicate): Seq[Predicate] = predicate match {
    case left And right => splitConjunction(left) ++ splitConjunction(right)
    case _              => predicate :: Nil
  }

  private[scraper] def splitDisjunction(predicate: Predicate): Seq[Predicate] = predicate match {
    case left Or right => splitDisjunction(left) ++ splitDisjunction(right)
    case _             => predicate :: Nil
  }
}

trait UnaryPredicate extends Predicate with UnaryExpression

trait LeafPredicate extends Predicate with LeafExpression
