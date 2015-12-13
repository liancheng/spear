package scraper.expressions

import scala.util.{Failure, Success, Try}

import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.promoteDataType
import scraper.types.{DataType, FractionalType, IntegralType, NumericType}

trait ArithmeticExpression extends Expression {
  lazy val numeric = dataType match {
    case t: NumericType => t.genericNumeric
  }
}

case class Negate(child: Expression) extends UnaryExpression with ArithmeticExpression {
  override lazy val strictlyTypedForm: Try[Expression] = for {
    strictChild <- child.strictlyTypedForm map {
      case NumericType(e)            => e
      case NumericType.Implicitly(e) => promoteDataType(e, NumericType.defaultType)
      case e                         => throw new TypeMismatchException(e, classOf[NumericType])
    }
  } yield if (strictChild sameOrEqual child) this else copy(child = strictChild)

  override lazy val dataType: DataType = whenStrictlyTyped(child.dataType)

  override def nullSafeEvaluate(value: Any): Any = numeric.negate(value)

  override def annotatedString: String = s"(-${child.annotatedString})"

  override def sql: String = s"(-${child.sql})"
}

trait BinaryArithmeticExpression extends ArithmeticExpression with BinaryExpression {
  override lazy val strictlyTypedForm: Try[Expression] = {
    val checkBranch: Expression => Try[Expression] = {
      case NumericType(e)            => Success(e)
      case NumericType.Implicitly(e) => Success(e)
      case e                         => Failure(new TypeMismatchException(e, classOf[NumericType]))
    }

    for {
      lhs <- left.strictlyTypedForm flatMap checkBranch
      rhs <- right.strictlyTypedForm flatMap checkBranch

      // Figures out the final data type of this arithmetic expression. Basically there are two
      // cases:
      //
      //  - The data type of at least one side is NumericType.  In this case, we use the wider type
      //    of the two as the final data type.
      //
      //  - The data type of neither side is NumericType, but both can be converted to NumericType
      //    implicitly.  In this case, we use the default NumericType as the final data type.
      t <- (lhs.dataType, rhs.dataType) match {
        case (t1: NumericType, t2) => t1 widest t2
        case (t1, t2: NumericType) => t1 widest t2
        case (t1, t2)              => Success(NumericType.defaultType)
      }

      newChildren = promoteDataType(lhs, t) :: promoteDataType(rhs, t) :: Nil
    } yield if (sameChildren(newChildren)) this else makeCopy(newChildren)
  }

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
      case t: FractionalType => t.genericFractional.div _
      case t: IntegralType   => t.genericIntegral.quot _
    }
  }

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = if (rhs == 0) null else div(lhs, rhs)

  override def annotatedString: String = s"(${left.annotatedString} / ${right.annotatedString})"

  override def sql: String = s"(${left.sql} / ${right.sql})"
}
