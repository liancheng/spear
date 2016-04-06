package scraper.expressions

import scraper.config.Settings
import scraper.parser.Parser

package object functions {
  def col(name: String): UnresolvedAttribute = new Parser(Settings.empty).parseAttribute(name)

  def lit(value: Any): Literal = Literal(value)

  def not(predicate: Expression): Not = Not(predicate)

  def coalesce(first: Expression, second: Expression, rest: Expression*): Coalesce =
    Coalesce(Seq(first, second) ++ rest)

  def rand(seed: Int): Rand = Rand(seed)

  def count(expression: Expression): Count = Count(expression)

  def average(expression: Expression): Average = Average(expression)

  def avg(expression: Expression): Average = average(expression)

  def sum(expression: Expression): Sum = Sum(expression)

  def max(expression: Expression): Max = Max(expression)

  def min(expression: Expression): Min = Min(expression)

  def bool_and(expression: Expression): BoolAnd = BoolAnd(expression)

  def bool_or(expression: Expression): BoolOr = BoolOr(expression)

  def distinct(agg: AggregateFunction): DistinctAggregateFunction = DistinctAggregateFunction(agg)

  def when(condition: Expression)(consequence: => Expression): IfBuilder =
    IfBuilder(condition, consequence)

  case class IfBuilder(condition: Expression, consequence: Expression) {
    def otherwise(alternative: => Expression): If = If(condition, consequence, alternative)
  }
}
