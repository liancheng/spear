package scraper.expressions

import scraper.types.{ DataType, NumericType }

trait ArithmeticExpression extends Expression {
  lazy val numeric = dataType.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]
}

case class Add(left: Expression, right: Expression)
  extends ArithmeticExpression
  with BinaryExpression {

  override def dataType: DataType = left.dataType

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.plus(lhs, rhs)

  override def nodeDescription: String = s"(${left.nodeDescription} + ${right.nodeDescription})"
}

case class Minus(left: Expression, right: Expression)
  extends ArithmeticExpression
  with BinaryExpression {

  override def dataType: DataType = left.dataType

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.minus(lhs, rhs)

  override def nodeDescription: String = s"(${left.nodeDescription} - ${right.nodeDescription})"
}

case class Multiply(left: Expression, right: Expression)
  extends ArithmeticExpression
  with BinaryExpression {

  override def dataType: DataType = left.dataType

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.times(lhs, rhs)

  override def nodeDescription: String = s"(${left.nodeDescription} * ${right.nodeDescription})"
}
