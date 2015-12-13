package scraper.expressions.dsl

import scraper.expressions._
import scraper.types.DataType

trait ExpressionDSL
  extends ArithmeticExpressionDSL
  with ComparisonDSL
  with LogicalOperatorDSL { this: Expression =>

  def as(alias: String): Alias = Alias(alias, this)

  def as(alias: Symbol): Alias = Alias(alias.name, this)

  def cast(dataType: DataType): Cast = Cast(this, dataType)
}
