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
    (promotedLhs, promotedRhs) <- promoteDataTypes(lhs, rhs)
    newChildren = promotedLhs :: promotedRhs :: Nil
  } yield if (sameChildren(newChildren)) this else makeCopy(newChildren)
}

case class Add(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def dataType: DataType = whenStrictlyTyped(left.dataType)

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.plus(lhs, rhs)

  override def nodeCaption: String =
    s"(${left.nodeCaption} + ${right.nodeCaption})"
}

case class Minus(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def dataType: DataType = whenStrictlyTyped(left.dataType)

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.minus(lhs, rhs)

  override def nodeCaption: String =
    s"(${left.nodeCaption} - ${right.nodeCaption})"
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def dataType: DataType = whenStrictlyTyped(left.dataType)

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.times(lhs, rhs)

  override def nodeCaption: String =
    s"(${left.nodeCaption} * ${right.nodeCaption})"
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def dataType: DataType = ???

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ???

  override def nodeCaption: String =
    s"(${left.nodeCaption} / ${right.nodeCaption})"
}
