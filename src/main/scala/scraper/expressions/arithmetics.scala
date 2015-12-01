package scraper.expressions

import scala.util.{Success, Try}

import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.{promoteDataType, widestTypeOf}
import scraper.types.{DataType, FractionalType, IntegralType, NumericType}

trait ArithmeticExpression extends Expression {
  lazy val numeric = dataType.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]
}

trait BinaryArithmeticExpression extends ArithmeticExpression with BinaryExpression {
  override lazy val strictlyTypedForm: Try[Expression] = for {
    lhs <- left.strictlyTypedForm map {
      case NumericType(e)            => e
      case NumericType.Implicitly(e) => e
      case e                         => throw new TypeMismatchException(e, classOf[NumericType])
    }

    rhs <- right.strictlyTypedForm map {
      case NumericType(e)            => e
      case NumericType.Implicitly(e) => e
      case e                         => throw new TypeMismatchException(e, classOf[NumericType])
    }

    // Figures out the final data type of this arithmetic expression. Basically there are two cases:
    //
    //  - The data type of at least one of both sides is NumericType.  In this case, we use the
    //    wider type of both sides as the final data type.
    //
    //  - The data type of neither side is NumericType, but both can be converted to NumericType
    //    implicitly.  In this case, we use the default NumericType as the final data type.
    t <- (lhs.dataType, rhs.dataType) match {
      case (t1: NumericType, t2) => widestTypeOf(t1, t2)
      case (t1, t2: NumericType) => widestTypeOf(t1, t2)
      case (t1, t2)              => Success(NumericType.defaultType)
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
