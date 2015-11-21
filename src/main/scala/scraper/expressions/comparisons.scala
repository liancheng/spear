package scraper.expressions

import scraper.expressions.Cast.implicitlyConvertible
import scraper.types.PrimitiveType

trait BinaryComparison extends Predicate with BinaryExpression {
  protected lazy val ordering: Ordering[Any] = whenTypeChecked(left.dataType match {
    case t: PrimitiveType => t.ordering.asInstanceOf[Ordering[Any]]
  })

  override def typeChecked: Boolean = childrenTypeChecked && (
    implicitlyConvertible(left.dataType, right.dataType) ||
    implicitlyConvertible(right.dataType, left.dataType)
  )

  override protected def casted: this.type = (left.dataType, right.dataType) match {
    case (lhsType, rhsType) if lhsType == rhsType =>
      this

    case (lhsType, rhsType) if implicitlyConvertible(lhsType, rhsType) =>
      makeCopy(Cast(left, rhsType) :: right :: Nil)

    case (lhsType, rhsType) if implicitlyConvertible(rhsType, lhsType) =>
      makeCopy(left :: Cast(right, lhsType) :: Nil)
  }
}

case class Eq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = lhs == rhs

  override def caption: String = s"(${left.caption} = ${right.caption})"
}

case class NotEq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = lhs != rhs

  override def caption: String = s"(${left.caption} != ${right.caption})"
}

case class Gt(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.gt(lhs, rhs)

  override def caption: String = s"(${left.caption} > ${right.caption}"
}

case class Lt(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.lt(lhs, rhs)

  override def caption: String = s"(${left.caption} < ${right.caption}"
}

case class GtEq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.gteq(lhs, rhs)

  override def caption: String = s"(${left.caption} >= ${right.caption}"
}

case class LtEq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.lteq(lhs, rhs)

  override def caption: String = s"(${left.caption} <= ${right.caption}"
}
