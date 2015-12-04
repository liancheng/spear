package scraper.expressions

import scala.util.Try

import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.{promoteDataType, widestTypeOf}
import scraper.types.{BooleanType, DataType, PrimitiveType}

trait BinaryComparison extends BinaryExpression {
  protected lazy val ordering: Ordering[Any] = whenStrictlyTyped {
    left.dataType match {
      case t: PrimitiveType => t.genericOrdering
    }
  }

  override lazy val strictlyTypedForm: Try[Expression] = for {
    lhs <- left.strictlyTypedForm map {
      case PrimitiveType(e) => e
      case e                => throw new TypeMismatchException(e, classOf[PrimitiveType])
    }

    rhs <- right.strictlyTypedForm map {
      case PrimitiveType(e) => e
      case e                => throw new TypeMismatchException(e, classOf[PrimitiveType])
    }

    t <- widestTypeOf(lhs.dataType, rhs.dataType)

    newChildren = promoteDataType(lhs, t) :: promoteDataType(rhs, t) :: Nil
  } yield if (sameChildren(newChildren)) this else makeCopy(newChildren)
}

case class Eq(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = BooleanType

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = lhs == rhs

  override def annotatedString: String = s"(${left.annotatedString} = ${right.annotatedString})"

  override def sql: String = s"(${left.sql} = ${right.sql})"
}

case class NotEq(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = BooleanType

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = lhs != rhs

  override def annotatedString: String = s"(${left.annotatedString} != ${right.annotatedString})"

  override def sql: String = s"(${left.sql} != ${right.sql})"
}

case class Gt(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = BooleanType

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.gt(lhs, rhs)

  override def annotatedString: String = s"(${left.annotatedString} > ${right.annotatedString})"

  override def sql: String = s"(${left.sql} > ${right.sql})"
}

case class Lt(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = BooleanType

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.lt(lhs, rhs)

  override def annotatedString: String = s"(${left.annotatedString} < ${right.annotatedString})"

  override def sql: String = s"(${left.sql} < ${right.sql})"
}

case class GtEq(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = BooleanType

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.gteq(lhs, rhs)

  override def annotatedString: String = s"(${left.annotatedString} >= ${right.annotatedString})"

  override def sql: String = s"(${left.sql} >= ${right.sql})"
}

case class LtEq(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = BooleanType

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.lteq(lhs, rhs)

  override def annotatedString: String = s"(${left.annotatedString} <= ${right.annotatedString})"

  override def sql: String = s"(${left.sql} <= ${right.sql})"
}
