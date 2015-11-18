package scraper.expressions

import scraper.expressions.Cast.implicitlyCastable
import scraper.types.{ DataType, NumericType }

trait ArithmeticExpression extends Expression {
  lazy val numeric = dataType.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]
}

trait BinaryArithmeticExpression extends ArithmeticExpression with BinaryExpression {
  override def typeChecked: Boolean =
    childrenTypeChecked && childrenTypes.forall(_.isInstanceOf[NumericType])

  override protected def casted: this.type = (left.dataType, right.dataType) match {
    case _ if left.dataType == right.dataType =>
      this

    case (lhsType, rhsType) if implicitlyCastable(lhsType, rhsType) =>
      makeCopy(Cast(left, rhsType) :: right :: Nil)

    case (lhsType, rhsType) if implicitlyCastable(rhsType, lhsType) =>
      makeCopy(left :: Cast(right, lhsType) :: Nil)
  }
}

case class Add(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def dataType: DataType = whenTypeChecked(left.dataType)

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.plus(lhs, rhs)

  override def caption: String = s"(${left.caption} + ${right.caption})"
}

case class Minus(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def dataType: DataType = whenTypeChecked(left.dataType)

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.minus(lhs, rhs)

  override def caption: String = s"(${left.caption} - ${right.caption})"
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def dataType: DataType = whenTypeChecked(left.dataType)

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.times(lhs, rhs)

  override def caption: String = s"(${left.caption} * ${right.caption})"
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def dataType: DataType = ???

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ???

  override def caption: String = s"(${left.caption} / ${right.caption})"
}
