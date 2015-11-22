package scraper.expressions

import scala.util.Try

import scraper.expressions.Cast.promoteDataTypes
import scraper.types.{DataType, NumericType}

trait ArithmeticExpression extends Expression {
  lazy val numeric = dataType.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]
}

trait BinaryArithmeticExpression extends ArithmeticExpression with BinaryExpression {
  override lazy val strictlyTyped: Try[Expression] = for {
    NumericType(lhs) <- left.strictlyTyped
    NumericType(rhs) <- right.strictlyTyped
    (e1, e2) <- promoteDataTypes(lhs, rhs)
  } yield makeCopy(e1 :: e2 :: Nil)
}

case class Add(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def dataType: DataType = whenStrictlyTyped(left.dataType)

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.plus(lhs, rhs)

  override def caption: String = s"(${left.caption} + ${right.caption})"
}

case class Minus(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def dataType: DataType = whenStrictlyTyped(left.dataType)

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.minus(lhs, rhs)

  override def caption: String = s"(${left.caption} - ${right.caption})"
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def dataType: DataType = whenStrictlyTyped(left.dataType)

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.times(lhs, rhs)

  override def caption: String = s"(${left.caption} * ${right.caption})"
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def dataType: DataType = ???

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ???

  override def caption: String = s"(${left.caption} / ${right.caption})"
}
