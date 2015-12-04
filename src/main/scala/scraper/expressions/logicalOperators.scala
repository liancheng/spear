package scraper.expressions

import scala.util.{Failure, Success, Try}

import scraper.Row
import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.{promoteDataType, widestTypeOf}
import scraper.types.{BooleanType, DataType}

trait BinaryLogicalPredicate extends BinaryExpression {
  override lazy val strictlyTypedForm: Try[Expression] = {
    val checkBranch: Expression => Try[Expression] = {
      case BooleanType(e)            => Success(e)
      case BooleanType.Implicitly(e) => Success(promoteDataType(e, BooleanType))
      case e =>
        Failure(new TypeMismatchException(e, BooleanType.getClass))
    }

    for {
      lhs <- left.strictlyTypedForm flatMap checkBranch
      rhs <- right.strictlyTypedForm flatMap checkBranch
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

  override def nullSafeEvaluate(value: Any): Any = !value.asInstanceOf[Boolean]

  override def annotatedString: String = s"(NOT ${child.annotatedString})"

  override lazy val strictlyTypedForm: Try[Expression] = for {
    e <- child.strictlyTypedForm map {
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

  override lazy val strictlyTypedForm: Try[Expression] = for {
    c <- condition.strictlyTypedForm map {
      case BooleanType(e)            => e
      case BooleanType.Implicitly(e) => promoteDataType(e, BooleanType)
      case e                         => throw new TypeMismatchException(e, BooleanType.getClass)
    }

    yes <- trueValue.strictlyTypedForm
    no <- falseValue.strictlyTypedForm
    t <- widestTypeOf(yes.dataType, no.dataType)

    newChildren = c :: promoteDataType(yes, t) :: promoteDataType(no, t) :: Nil
  } yield if (sameChildren(newChildren)) this else makeCopy(newChildren)

  override def evaluate(input: Row): Any =
    if (condition.evaluate(input).asInstanceOf[Boolean]) {
      trueValue.evaluate(input)
    } else {
      falseValue.evaluate(input)
    }
}
