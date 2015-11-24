package scraper.expressions

import scala.util.Try

import scraper.TypeMismatchException
import scraper.expressions.Cast.promoteDataTypes
import scraper.types.{IntegralType, FractionalType, DataType, NumericType}

trait ArithmeticExpression extends Expression {
  lazy val numeric = dataType.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]
}

trait BinaryArithmeticExpression extends ArithmeticExpression with BinaryExpression {
  override lazy val strictlyTyped: Try[Expression] = for {
    lhs <- left.strictlyTyped map {
      case NumericType(e) => e
      case e              => throw TypeMismatchException(e, classOf[NumericType], None)
    }
    rhs <- right.strictlyTyped map {
      case NumericType(e) => e
      case e              => throw TypeMismatchException(e, classOf[NumericType], None)
    }
    (promotedLhs, promotedRhs) <- promoteDataTypes(lhs, rhs)
    newChildren = promotedLhs :: promotedRhs :: Nil
  } yield if (sameChildren(newChildren)) this else makeCopy(newChildren)

  override lazy val dataType: DataType = whenStrictlyTyped(left.dataType)
}

case class Add(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.plus(lhs, rhs)

  override def nodeCaption: String =
    s"(${left.nodeCaption} + ${right.nodeCaption})"
}

case class Minus(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.minus(lhs, rhs)

  override def nodeCaption: String =
    s"(${left.nodeCaption} - ${right.nodeCaption})"
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.times(lhs, rhs)

  override def nodeCaption: String =
    s"(${left.nodeCaption} * ${right.nodeCaption})"
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  private lazy val div = whenStrictlyTyped {
    dataType match {
      case t: FractionalType => t.fractional.asInstanceOf[Fractional[Any]].div _
      case t: IntegralType   => t.integral.asInstanceOf[Integral[Any]].quot _
    }
  }

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = if (rhs == 0) null else div(lhs, rhs)

  override def nodeCaption: String =
    s"(${left.nodeCaption} / ${right.nodeCaption})"
}
