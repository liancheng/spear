package scraper.expressions

import scala.util.{Failure, Success, Try}

import scraper.Row
import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.promoteDataType
import scraper.types.{BooleanType, DataType}

trait BinaryLogicalPredicate extends BinaryExpression {
  override lazy val strictlyTypedForm: Try[Expression] = {
    val checkBranch: Expression => Try[Expression] = {
      case BooleanType.Implicitly(e) => Success(promoteDataType(e, BooleanType))
      case e => Failure(
        new TypeMismatchException(e, BooleanType.getClass)
      )
    }

    for {
      lhs <- left.strictlyTypedForm flatMap checkBranch
      rhs <- right.strictlyTypedForm flatMap checkBranch
      newChildren = lhs :: rhs :: Nil
    } yield if (sameChildren(newChildren)) this else makeCopy(newChildren)
  }

  override def dataType: DataType = BooleanType
}

case class And(left: Expression, right: Expression) extends BinaryLogicalPredicate {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = {
    lhs.asInstanceOf[Boolean] && rhs.asInstanceOf[Boolean]
  }

  override def annotatedString: String = s"(${left.annotatedString} AND ${right.annotatedString})"

  override def sql: String = s"(${left.sql} AND ${right.sql})"
}

case class Or(left: Expression, right: Expression) extends BinaryLogicalPredicate {
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
      case BooleanType.Implicitly(e) => promoteDataType(e, BooleanType)
      case e                         => throw new TypeMismatchException(e, BooleanType.getClass)
    }
  } yield copy(child = e)

  override def sql: String = s"(NOT ${child.sql})"
}

case class If(condition: Expression, yes: Expression, no: Expression)
  extends Expression {

  override protected def strictDataType: DataType = yes.dataType

  override def children: Seq[Expression] = Seq(condition, yes, no)

  override def annotatedString: String =
    s"if (${condition.annotatedString}) ${yes.annotatedString} else ${no.annotatedString}"

  override def sql: String = s"IF(${condition.sql}, ${yes.sql}, ${no.sql})"

  override lazy val strictlyTypedForm: Try[If] = for {
    strictCondition <- condition.strictlyTypedForm map {
      case BooleanType.Implicitly(e) => promoteDataType(e, BooleanType)
      case e                         => throw new TypeMismatchException(e, BooleanType.getClass)
    }

    strictYes <- yes.strictlyTypedForm
    strictNo <- no.strictlyTypedForm
    finalType <- strictYes.dataType widest strictNo.dataType

    promotedYes = promoteDataType(strictYes, finalType)
    promotedNo = promoteDataType(strictNo, finalType)
    newChildren = strictCondition :: promotedYes :: promotedNo :: Nil
  } yield if (sameChildren(newChildren)) this else copy(
    condition = strictCondition, yes = promotedYes, no = promotedNo
  )

  override def evaluate(input: Row): Any =
    if (condition.evaluate(input).asInstanceOf[Boolean]) {
      yes.evaluate(input)
    } else {
      no.evaluate(input)
    }
}
