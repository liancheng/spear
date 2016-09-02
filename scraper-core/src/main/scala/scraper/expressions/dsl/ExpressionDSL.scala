package scraper.expressions.dsl

import scraper.Name
import scraper.expressions._
import scraper.expressions.NamedExpression.newExpressionID
import scraper.types.DataType

trait ExpressionDSL
  extends ArithmeticExpressionDSL
  with ComparisonDSL
  with LogicalOperatorDSL { this: Expression =>

  def as(alias: Name): Alias = Alias(this, alias, newExpressionID())

  def cast(dataType: DataType): Cast = Cast(this, dataType)

  def isNull: IsNull = IsNull(this)

  def notNull: IsNotNull = IsNotNull(this)

  def asc: SortOrder = SortOrder(this, Ascending, nullsLarger = true)

  def desc: SortOrder = SortOrder(this, Descending, nullsLarger = true)

  def in(list: Seq[Expression]): In = In(this, list)

  def in(first: Expression, rest: Expression*): In = this in (first +: rest)

  def rlike(pattern: Expression): RLike = RLike(this, pattern)
}
