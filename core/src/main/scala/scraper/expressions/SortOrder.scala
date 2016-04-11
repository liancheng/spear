package scraper.expressions

import scraper.expressions.typecheck.{AllBelongTo, TypeConstraints}
import scraper.types.{DataType, OrderedType}

abstract sealed class SortDirection

case object Ascending extends SortDirection {
  override def toString: String = "ASC"
}

case object Descending extends SortDirection {
  override def toString: String = "DESC"
}

case class SortOrder(child: Expression, direction: SortDirection, nullsLarger: Boolean)
  extends UnaryExpression with UnevaluableExpression {

  override protected def typeConstraints: TypeConstraints = AllBelongTo(OrderedType, child :: Nil)

  override def dataType: DataType = child.dataType

  override def isNullable: Boolean = child.isNullable

  def nullsFirst: SortOrder = direction match {
    case Ascending  => copy(nullsLarger = false)
    case Descending => copy(nullsLarger = true)
  }

  def nullsLast: SortOrder = direction match {
    case Ascending  => copy(nullsLarger = true)
    case Descending => copy(nullsLarger = false)
  }

  def isAscending: Boolean = direction == Ascending

  def isNullsFirst: Boolean = isAscending ^ nullsLarger

  override protected def template(childString: String): String = {
    val nullsFirstOrLast = (direction, nullsLarger) match {
      case (Ascending, false) => "FIRST"
      case (Descending, true) => "FIRST"
      case _                  => "LAST"
    }

    s"$childString $direction NULLS $nullsFirstOrLast"
  }
}
