package scraper.expressions

import scala.math.pow
import scala.util.{Failure, Success, Try}

import scraper.{NullSafeOrdering, Row}
import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.widenDataType
import scraper.expressions.functions.lit
import scraper.expressions.typecheck.{StrictlyTyped, TypeConstraint}
import scraper.types._
import scraper.utils._

trait ArithmeticExpression extends Expression {
  lazy val numeric = dataType match {
    case t: NumericType => t.genericNumeric
  }
}

trait UnaryArithmeticOperator extends UnaryOperator with ArithmeticExpression {
  override protected def typeConstraint: TypeConstraint = children sameSubtypeOf NumericType

  override protected lazy val strictDataType: DataType = child.dataType
}

case class Negate(child: Expression) extends UnaryArithmeticOperator {
  override def operator: String = "-"

  override def nullSafeEvaluate(value: Any): Any = numeric.negate(value)
}

case class Positive(child: Expression) extends UnaryArithmeticOperator {
  override def operator: String = "+"

  override def nullSafeEvaluate(value: Any): Any = value
}

trait BinaryArithmeticOperator extends ArithmeticExpression with BinaryOperator {
  // Type constraint for a binary arithmetic operator requires that either of the following two
  // conditions must hold:
  //
  //  1. Both operands are of numeric types
  //
  //     In this case, the data type of this binary arithmetic operator is the wider one of the
  //     two numeric types.
  //
  //  2. One operand is of a numeric type `T` while the other one is of string type.
  //
  //     In this case, the data type of this binary arithmetic operator is `T`. The string operand
  //     will also be casted to `T`.
  //
  // For example, the following expressions are all valid:
  //
  //  - 1:INT + 2:BIGINT
  //  - 1:INT + '2':STRING
  //  - '1':STRING + 2:BIGINT
  //
  // while
  //
  //  - '1':STRING + '2':STRING
  //  - TRUE:BOOLEAN + '2':STRING
  //
  // are invalid. This behavior is consistent with PostgreSQL.
  override protected def typeConstraint: TypeConstraint = new TypeConstraint {
    override def enforced: Try[Seq[Expression]] = for {
      strictInput <- StrictlyTyped(children).enforced

      Seq(lhsType, rhsType) = strictInput map (_.dataType)

      resultType <- (lhsType, rhsType) match {
        case (_: NumericType, _: NumericType) => lhsType widest rhsType
        case (_: NumericType, StringType)     => Success(lhsType)
        case (StringType, _: NumericType)     => Success(rhsType)
        case _ =>
          Failure(new TypeMismatchException(
            s"""Operator $operator requires at least one operand to be of numeric type and all other
               |operands to be of either numeric type or string type. However, the left operand
               |$left is of ${left.dataType.sql} type and the right operand $right is of
               |${right.dataType.sql} type.
             """.oneLine
          ))
      }
    } yield strictInput map {
      case e if e.dataType == StringType => e cast resultType
      case e                             => widenDataType(e, resultType)
    }
  }

  override protected lazy val strictDataType: DataType = left.dataType
}

case class Plus(left: Expression, right: Expression) extends BinaryArithmeticOperator {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.plus(lhs, rhs)

  override def operator: String = "+"
}

case class Minus(left: Expression, right: Expression) extends BinaryArithmeticOperator {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.minus(lhs, rhs)

  override def operator: String = "-"
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmeticOperator {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = numeric.times(lhs, rhs)

  override def operator: String = "*"
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmeticOperator {
  private lazy val div = whenStrictlyTyped {
    dataType match {
      case t: FractionalType => t.genericFractional.div _
      case t: IntegralType   => t.genericIntegral.quot _
    }
  }

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = div(lhs, rhs)

  override def operator: String = "/"
}

case class Remainder(left: Expression, right: Expression) extends BinaryArithmeticOperator {
  override protected def typeConstraint: TypeConstraint =
    super.typeConstraint andAlso (_ sameSubtypeOf IntegralType)

  override protected lazy val strictDataType: DataType = left.dataType

  private lazy val integral = whenStrictlyTyped(dataType match {
    case t: IntegralType => t.genericIntegral
  })

  override def operator: String = "%"

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = integral.rem(lhs, rhs)
}

case class Power(left: Expression, right: Expression) extends BinaryArithmeticOperator {
  override protected def typeConstraint: TypeConstraint =
    super.typeConstraint andAlso (_ sameTypeAs DoubleType)

  override def dataType: DataType = DoubleType

  override def operator: String = "^"

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any =
    pow(lhs.asInstanceOf[Double], rhs.asInstanceOf[Double])
}

case class IsNaN(child: Expression) extends UnaryExpression {
  override protected def typeConstraint: TypeConstraint = child subtypeOf FractionalType

  override def dataType: DataType = BooleanType

  override def evaluate(input: Row): Any = {
    val value = child evaluate input
    if (value == null) false else (value, dataType) match {
      case (v: Double, DoubleType) => lit(v).isNaN
      case (v: Float, FloatType)   => lit(v).isNaN
      case _                       => false
    }
  }
}

abstract class GreatestLike extends Expression {
  assert(children.nonEmpty)

  protected def nullsLarger: Boolean

  override protected lazy val strictDataType: DataType = children.head.dataType

  override protected def typeConstraint: TypeConstraint = children sameSubtypeOf OrderedType

  protected lazy val ordering = new NullSafeOrdering(strictDataType, nullsLarger)
}

case class Greatest(children: Seq[Expression]) extends GreatestLike {
  override protected def nullsLarger: Boolean = false

  override def evaluate(input: Row): Any = children map (_ evaluate input) max ordering
}

object Greatest {
  def apply(first: Expression, rest: Expression*): Greatest = Greatest(first +: rest)
}

case class Least(children: Seq[Expression]) extends GreatestLike {
  override protected def nullsLarger: Boolean = true

  override def evaluate(input: Row): Any = children map (_ evaluate input) min ordering
}

object Least {
  def apply(first: Expression, rest: Expression*): Least = Least(first +: rest)
}
