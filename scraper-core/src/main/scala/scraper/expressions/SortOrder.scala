package scraper.expressions

import scraper.expressions.typecheck.TypeConstraint
import scraper.types.{DataType, OrderedType}

sealed trait SortDirection

case object Ascending extends SortDirection {
  override def toString: String = "ASC"
}

case object Descending extends SortDirection {
  override def toString: String = "DESC"
}

case class SortOrder(child: Expression, direction: SortDirection, isNullLarger: Boolean)
  extends UnaryExpression with UnevaluableExpression {

  override def dataType: DataType = child.dataType

  override def isNullable: Boolean = child.isNullable

  def nullsFirst: SortOrder = copy(isNullLarger = !isAscending)

  def nullsLast: SortOrder = copy(isNullLarger = isAscending)

  def nullsLarger: SortOrder = copy(isNullLarger = true)

  def isAscending: Boolean = direction == Ascending

  def isNullsFirst: Boolean = isAscending ^ isNullLarger

  override protected lazy val typeConstraint: TypeConstraint = child subtypeOf OrderedType

  override protected def template(childString: String): String =
    s"$childString $direction NULLS ${if (isNullsFirst) "FIRST" else "LAST"}"
}
