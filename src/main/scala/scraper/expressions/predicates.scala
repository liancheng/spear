package scraper.expressions

import scraper.types.{BooleanType, DataType}

trait Predicate extends Expression {
  override def dataType: DataType = BooleanType

  def &&(that: Predicate): And = And(this, that)

  def and(that: Predicate): And = this && that

  def ||(that: Predicate): Or = Or(this, that)

  def or(that: Predicate): Or = this || that

  def unary_! : Not = Not(this)
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

  def unapply(e: Expression): Option[Predicate] = e match {
    case e: Predicate => Some(e)
    case _            => None
  }
}

trait UnaryPredicate extends Predicate with UnaryExpression

trait LeafPredicate extends Predicate with LeafExpression
