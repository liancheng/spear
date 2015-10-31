package scraper.expressions

import scraper.Row
import scraper.types.{ BooleanType, DataType }

trait Predicate extends Expression {
  override def dataType: DataType = BooleanType
}

trait BinaryLogicalPredicate extends Predicate with BinaryExpression

trait UnaryPredicate extends Predicate with UnaryExpression {
}

case class And(left: Expression, right: Expression) extends Predicate with BinaryExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = {
    lhs.asInstanceOf[Boolean] && rhs.asInstanceOf[Boolean]
  }
}

case class Or(left: Expression, right: Expression) extends Predicate with BinaryExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = {
    lhs.asInstanceOf[Boolean] || rhs.asInstanceOf[Boolean]
  }
}

case class Not(child: Expression) extends UnaryPredicate {
  override def evaluate(input: Row): Any = !child.evaluate(input).asInstanceOf[Boolean]
}

case class EqualTo(left: Expression, right: Expression) extends Predicate with BinaryExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = lhs == rhs
}
