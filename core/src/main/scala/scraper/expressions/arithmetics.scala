package scraper.expressions

import scala.util.{Failure, Success, Try}

import scraper.{NullSafeOrdering, Row}
import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.{promoteDataType, widestTypeOf}
import scraper.types._
import scraper.utils._

trait ArithmeticExpression extends Expression {
  lazy val numeric = dataType match {
    case t: NumericType => t.genericNumeric
  }
}

trait UnaryArithmeticOperator extends UnaryOperator with ArithmeticExpression {
  override lazy val strictlyTyped: Try[Expression] = for {
    strictChild <- child.strictlyTyped map {
      case NumericType(e)            => e
      case NumericType.Implicitly(e) => promoteDataType(e, NumericType.defaultType)
      case e                         => throw new TypeMismatchException(e, classOf[NumericType])
    }
  } yield if (strictChild same child) this else makeCopy(strictChild :: Nil)

  override protected def strictDataType: DataType = child.dataType
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
  override lazy val strictlyTyped: Try[Expression] = {
    val checkBranch: Expression => Try[Expression] = {
      case NumericType(e)            => Success(e)
      case NumericType.Implicitly(e) => Success(promoteDataType(e, NumericType.defaultType))
      case e                         => Failure(new TypeMismatchException(e, classOf[NumericType]))
    }

    for {
      lhs <- left.strictlyTyped flatMap checkBranch
      rhs <- right.strictlyTyped flatMap checkBranch

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

  override protected def strictDataType: DataType = left.dataType
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

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = if (rhs == 0) null else div(lhs, rhs)

  override def operator: String = "/"
}

case class IsNaN(child: Expression) extends UnaryExpression {
  override lazy val strictlyTyped: Try[Expression] = for {
    strictChild <- child.strictlyTyped map {
      case FractionalType(e)            => e
      case FractionalType.Implicitly(e) => e
      case e =>
        throw new TypeMismatchException(e, classOf[FractionalType])
    }

    finalType = strictChild.dataType match {
      case t: FractionalType            => t
      case FractionalType.Implicitly(t) => FractionalType.defaultType
    }

    promotedChild = promoteDataType(strictChild, finalType)
  } yield if (promotedChild same child) this else copy(child = promotedChild)

  override def dataType: DataType = BooleanType

  override def evaluate(input: Row): Any = {
    val value = child evaluate input
    if (value == null) false else (value, dataType) match {
      case (v: Double, DoubleType) => v.isNaN
      case (v: Float, FloatType)   => v.isNaN
      case _                       => false
    }
  }
}

abstract class GreatestLike extends Expression {
  assert(children.nonEmpty)

  def nullLarger: Boolean

  override protected def strictDataType: DataType = children.head.dataType

  override lazy val strictlyTyped: Try[Expression] = for {
    strictChildren <- sequence(children map (_.strictlyTyped))
    widestType <- widestTypeOf(strictChildren map (_.dataType)) map {
      case t: OrderedType => t
      case t              => throw new TypeMismatchException(this, classOf[OrderedType])
    }
    promotedChildren = strictChildren.map(promoteDataType(_, widestType))
  } yield if (sameChildren(promotedChildren)) this else makeCopy(promotedChildren)

  protected lazy val ordering = new NullSafeOrdering(strictDataType, nullLarger)
}

case class Greatest(children: Seq[Expression], nullLarger: Boolean) extends GreatestLike {
  override def evaluate(input: Row): Any = children map (_ evaluate input) max ordering
}

case class Least(children: Seq[Expression], nullLarger: Boolean) extends GreatestLike {
  override def evaluate(input: Row): Any = children map (_ evaluate input) min ordering
}
