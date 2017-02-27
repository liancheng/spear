package scraper.expressions

import scraper.Row
import scraper.expressions.typecheck.TypeConstraint
import scraper.types.{BooleanType, DataType, OrderedType}

trait BinaryComparison extends BinaryOperator {
  override def dataType: DataType = BooleanType

  protected lazy val ordering: Ordering[Any] = whenStrictlyTyped {
    OrderedType.orderingOf(left.dataType)
  }

  override protected lazy val typeConstraint: TypeConstraint = children sameSubtypeOf OrderedType
}

case class Eq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.equiv(lhs, rhs)

  override def operator: String = "="
}

case class NotEq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = !ordering.equiv(lhs, rhs)

  override def operator: String = "<>"
}

case class NullSafeEq(left: Expression, right: Expression) extends BinaryComparison {
  override def isNullable: Boolean = false

  override def evaluate(input: Row): Any = children map (_ evaluate input) match {
    case Seq(null, null) => true
    case Seq(null, _)    => false
    case Seq(_, null)    => false
    case Seq(lhs, rhs)   => ordering.equiv(lhs, rhs)
  }

  override def operator: String = "<=>"
}

case class Gt(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.gt(lhs, rhs)

  override def operator: String = ">"
}

case class Lt(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.lt(lhs, rhs)

  override def operator: String = "<"
}

case class GtEq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.gteq(lhs, rhs)

  override def operator: String = ">="
}

case class LtEq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.lteq(lhs, rhs)

  override def operator: String = "<="
}

case class In(test: Expression, list: Seq[Expression]) extends Expression {
  override def children: Seq[Expression] = test +: list

  override protected lazy val strictDataType: DataType = BooleanType

  override protected lazy val typeConstraint: TypeConstraint =
    test.anyType concat (list sameTypeAs test.dataType)

  override def evaluate(input: Row): Any = {
    val testValue = test evaluate input
    val listValues = list map { _ evaluate input }
    listValues contains testValue
  }

  override protected def template(childList: Seq[String]): String = {
    val Seq(testString, listString @ _*) = childList
    s"($testString IN (${listString mkString ", "}))"
  }
}
