package scraper.expressions

import scraper.Row
import scraper.types.{ BooleanType, DataType }

trait Predicate extends Expression {
  override def dataType: DataType = BooleanType

  def &&(that: Predicate): And = And(this, that)

  def ||(that: Predicate): Or = Or(this, that)

  def unary_!(that: Predicate): Not = Not(this)
}

object Predicate {
  private[scraper] def splitConjunction(predicate: Predicate): Seq[Predicate] = predicate match {
    case And(left, right) => splitConjunction(left) ++ splitConjunction(right)
    case _                => predicate :: Nil
  }

  private[scraper] def splitDisjunction(predicate: Predicate): Seq[Predicate] = predicate match {
    case Or(left, right) => splitDisjunction(left) ++ splitDisjunction(right)
    case _               => predicate :: Nil
  }
}

trait BinaryLogicalPredicate extends Predicate with BinaryExpression

trait UnaryPredicate extends Predicate with UnaryExpression

trait LeafPredicate extends Predicate with LeafExpression

case class And(left: Predicate, right: Predicate) extends Predicate with BinaryExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = {
    lhs.asInstanceOf[Boolean] && rhs.asInstanceOf[Boolean]
  }

  override def caption: String = s"(${left.caption} AND ${right.caption})"
}

case class Or(left: Predicate, right: Predicate) extends Predicate with BinaryExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = {
    lhs.asInstanceOf[Boolean] || rhs.asInstanceOf[Boolean]
  }

  override def caption: String = s"(${left.caption} OR ${right.caption})"
}

case class Not(child: Predicate) extends UnaryPredicate {
  override def evaluate(input: Row): Any = !child.evaluate(input).asInstanceOf[Boolean]

  override def caption: String = s"(NOT ${child.caption})"
}

case class EqualTo(left: Expression, right: Expression) extends Predicate with BinaryExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = lhs == rhs

  override def caption: String = s"(${left.caption} = ${right.caption})"
}

case class NotEqualTo(left: Expression, right: Expression) extends Predicate with BinaryExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = lhs != rhs

  override def caption: String = s"(${left.caption} <> ${right.caption})"
}
