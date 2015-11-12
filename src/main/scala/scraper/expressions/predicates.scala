package scraper.expressions

import scraper.Row
import scraper.types.{ BooleanType, DataType }

trait Predicate extends Expression {
  override def dataType: DataType = BooleanType

  def &&(that: Predicate): And = And(this, that)

  def ||(that: Predicate): Or = Or(this, that)

  def unary_!(that: Predicate): Not = Not(this)
}

trait BinaryLogicalPredicate extends Predicate with BinaryExpression

trait UnaryPredicate extends Predicate with UnaryExpression {
}

case class And(left: Expression, right: Expression) extends Predicate with BinaryExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = {
    lhs.asInstanceOf[Boolean] && rhs.asInstanceOf[Boolean]
  }

  override def nodeDescription: String = s"(${left.nodeDescription} AND ${right.nodeDescription})"
}

case class Or(left: Expression, right: Expression) extends Predicate with BinaryExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = {
    lhs.asInstanceOf[Boolean] || rhs.asInstanceOf[Boolean]
  }

  override def nodeDescription: String = s"(${left.nodeDescription} OR ${right.nodeDescription})"
}

case class Not(child: Expression) extends UnaryPredicate {
  override def evaluate(input: Row): Any = !child.evaluate(input).asInstanceOf[Boolean]

  override def nodeDescription: String = s"(NOT ${child.nodeDescription})"
}

case class EqualTo(left: Expression, right: Expression) extends Predicate with BinaryExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = lhs == rhs

  override def nodeDescription: String = s"(${left.nodeDescription} = ${right.nodeDescription})"
}

case class NotEqualTo(left: Expression, right: Expression) extends Predicate with BinaryExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = lhs != rhs

  override def nodeDescription: String = s"(${left.nodeDescription} <> ${right.nodeDescription})"
}
