package scraper.expressions

import scala.util.Try

import scraper.expressions.Cast.{commonTypeOf, promoteDataType}
import scraper.types.{BooleanType, DataType}
import scraper.{Row, TypeMismatchException}

trait BinaryLogicalPredicate extends BinaryExpression {
  override lazy val strictlyTyped: Try[Expression] = {
    for {
      lhs <- left.strictlyTyped map {
        case BooleanType(e)            => e
        case BooleanType.Implicitly(e) => promoteDataType(e, BooleanType)
        case e                         => throw new TypeMismatchException(e, BooleanType.getClass)
      }

      rhs <- right.strictlyTyped map {
        case BooleanType(e)            => e
        case BooleanType.Implicitly(e) => promoteDataType(e, BooleanType)
        case e                         => throw new TypeMismatchException(e, BooleanType.getClass)
      }

      newChildren = lhs :: rhs :: Nil
    } yield if (sameChildren(newChildren)) this else makeCopy(newChildren)
  }
}

case class And(left: Expression, right: Expression) extends BinaryLogicalPredicate {
  override def dataType: DataType = BooleanType

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = {
    lhs.asInstanceOf[Boolean] && rhs.asInstanceOf[Boolean]
  }

  override def annotatedString: String = s"(${left.annotatedString} AND ${right.annotatedString})"

  override def sql: String = s"(${left.sql} AND ${right.sql})"
}

case class Or(left: Expression, right: Expression) extends BinaryLogicalPredicate {
  override def dataType: DataType = BooleanType

  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any =
    lhs.asInstanceOf[Boolean] || rhs.asInstanceOf[Boolean]

  override def annotatedString: String = s"(${left.annotatedString} OR ${right.annotatedString})"

  override def sql: String = s"(${left.sql} OR ${right.sql})"
}

case class Not(child: Expression) extends UnaryExpression {
  override def dataType: DataType = BooleanType

  override def evaluate(input: Row): Any = !child.evaluate(input).asInstanceOf[Boolean]

  override def annotatedString: String = s"(NOT ${child.annotatedString})"

  override lazy val strictlyTyped: Try[Expression] = for {
    e <- child.strictlyTyped map {
      case BooleanType(e)            => e
      case BooleanType.Implicitly(e) => promoteDataType(e, BooleanType)
      case e                         => throw new TypeMismatchException(e, BooleanType.getClass)
    }
  } yield copy(child = e)

  override def sql: String = s"(NOT ${child.sql})"
}

case class If(condition: Expression, trueValue: Expression, falseValue: Expression)
  extends Expression {

  override def dataType: DataType = whenStrictlyTyped(trueValue.dataType)

  override def children: Seq[Expression] = Seq(condition, trueValue, falseValue)

  override def annotatedString: String =
    s"if (${condition.annotatedString}) " +
      s"${trueValue.annotatedString} else ${falseValue.annotatedString}"

  override def sql: String = s"IF(${condition.sql}, ${trueValue.sql}, ${falseValue.sql})"

  override lazy val strictlyTyped: Try[Expression] = for {
    c <- condition.strictlyTyped map {
      case BooleanType(e)            => e
      case BooleanType.Implicitly(e) => promoteDataType(e, BooleanType)
      case e                         => throw new TypeMismatchException(e, BooleanType.getClass)
    }

    yes <- trueValue.strictlyTyped
    no <- falseValue.strictlyTyped
    t <- commonTypeOf(yes.dataType, no.dataType)

    newChildren = c :: promoteDataType(yes, t) :: promoteDataType(no, t) :: Nil
  } yield if (sameChildren(newChildren)) this else makeCopy(newChildren)

  override def evaluate(input: Row): Any =
    if (condition.evaluate(input).asInstanceOf[Boolean]) {
      trueValue.evaluate(input)
    } else {
      falseValue.evaluate(input)
    }
}
