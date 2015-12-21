package scraper.expressions

package object functions {
  def lit(value: Any): Literal = Literal(value)

  def not(predicate: Expression): Expression = Not(predicate)

  val count: Count = Count(Literal(1))

  def sum(expr: Expression): Sum = Sum(expr)

  def max(expr: Expression): Max = Max(expr)

  def min(expr: Expression): Min = Min(expr)

  def coalesce(first: Expression, second: Expression, rest: Expression*): Coalesce =
    Coalesce(Seq(first, second) ++ rest)
}
