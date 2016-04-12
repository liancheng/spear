package scraper.expressions

import scraper.types.{BooleanType, DataType, OrderedType}
import scraper.Row
import scraper.expressions.typecheck.TypeConstraints

trait BinaryComparison extends BinaryOperator {
  override def dataType: DataType = BooleanType

  protected lazy val ordering: Ordering[Any] = whenStrictlyTyped {
    left.dataType match {
      case t: OrderedType => t.genericOrdering
    }
  }

  override protected def typeConstraints: TypeConstraints = children subtypesOf OrderedType
}

object BinaryComparison {
  def unapply(e: Expression): Option[(Expression, Expression)] = e match {
    case c: BinaryComparison => Some((c.left, c.right))
    case _                   => None
  }
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

  override protected def strictDataType: DataType = BooleanType

  override protected def typeConstraints: TypeConstraints =
    (test ofType BooleanType) ~ list.allCompatible

  override def evaluate(input: Row): Any = {
    val testValue = test evaluate input
    val listValues = list map (_ evaluate input)

    dataType match {
      case t: OrderedType =>
        listValues exists (t.genericOrdering.compare(testValue, _) == 0)

      case _ =>
        false
    }
  }

  override protected def template(childList: Seq[String]): String = {
    val Seq(testString, listString @ _*) = childList
    s"($testString IN (${listString mkString ", "}))"
  }
}
