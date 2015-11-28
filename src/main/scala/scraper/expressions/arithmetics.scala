package scraper.expressions

import scala.util.Try

import scraper.TypeMismatchException
import scraper.expressions.Cast.{commonTypeOf, promoteDataType}
import scraper.types.{DataType, FractionalType, IntegralType, NumericType}

trait ArithmeticExpression extends Expression {
  lazy val numeric = dataType.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]
}

trait BinaryArithmeticExpression extends ArithmeticExpression with BinaryExpression {
  override lazy val strictlyTyped: Try[Expression] = for {
    // Data type of the left hand side must be either a NumericType or can be implicitly converted
    // to a NumericType.
    lhs <- left.strictlyTyped map {
      case NumericType(e)            => e
      case NumericType.Implicitly(e) => e
      case e                         => throw TypeMismatchException(e, classOf[NumericType], None)
    }

    // Data type of the right hand side must be either a NumericType or can be implicitly converted
    // to a NumericType.
    rhs <- right.strictlyTyped map {
      case NumericType(e)            => e
      case NumericType.Implicitly(e) => e
      case e                         => throw TypeMismatchException(e, classOf[NumericType], None)
    }

    t <- (lhs.dataType, rhs.dataType) match {
      // If the left hand side is of a NumericType while the right hand side is not, uses the
      // left hand side data type as the final data type
      case (t1: NumericType, t2) => commonTypeOf(t1, t2)

      // If the right hand side is of a NumericType while the left hand side is not, uses the
      // left hand side data type as the final data type
      case (t1, t2: NumericType) => commonTypeOf(t1, t2)

      // Otherwise, use NumericType.defaultType as the final data type
      case (t1, t2)              => Try(NumericType.defaultType)
    }

    newChildren = promoteDataType(lhs, t) :: promoteDataType(rhs, t) :: Nil
  } yield if (sameChildren(newChildren)) this else makeCopy(newChildren)

  override lazy val dataType: DataType = whenStrictlyTyped(left.dataType)
}

case class Add(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.plus(lhs, rhs)

  override def annotatedString: String = s"(${left.annotatedString} + ${right.annotatedString})"

  override def sql: String = s"(${left.sql} + ${right.sql})"
}

case class Minus(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.minus(lhs, rhs)

  override def annotatedString: String = s"(${left.annotatedString} - ${right.annotatedString})"

  override def sql: String = s"(${left.sql} - ${right.sql})"
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.times(lhs, rhs)

  override def annotatedString: String = s"(${left.annotatedString} * ${right.annotatedString})"

  override def sql: String = s"(${left.sql} * ${right.sql})"
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmeticExpression {
  private lazy val div = whenStrictlyTyped {
    dataType match {
      case t: FractionalType => t.fractional.asInstanceOf[Fractional[Any]].div _
      case t: IntegralType   => t.integral.asInstanceOf[Integral[Any]].quot _
    }
  }

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = if (rhs == 0) null else div(lhs, rhs)

  override def annotatedString: String = s"(${left.annotatedString} / ${right.annotatedString})"

  override def sql: String = s"(${left.sql} / ${right.sql})"
}
