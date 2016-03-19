package scraper.expressions.dsl

import scala.language.implicitConversions

import scraper.expressions._
import scraper.expressions.NamedExpression.newExpressionID
import scraper.types.DataType

trait ExpressionDSL
  extends ArithmeticExpressionDSL
  with ComparisonDSL
  with LogicalOperatorDSL { this: Expression =>

  def as(alias: String): Alias = Alias(this, alias, newExpressionID())

  def as(alias: Symbol): Alias = Alias(this, alias.name, newExpressionID())

  def cast(dataType: DataType): Cast = Cast(this, dataType)

  def isNull: IsNull = IsNull(this)

  def notNull: IsNotNull = IsNotNull(this)

  def asc: SortOrder = SortOrder(this, Ascending, nullsLarger = true)

  def desc: SortOrder = SortOrder(this, Descending, nullsLarger = true)

  def in(list: Seq[Expression]): In = In(this, list)

  def in(first: Expression, rest: Expression*): In = this in (first +: rest)

  def asGrouping: GroupingAlias = GroupingAlias(this)
}
