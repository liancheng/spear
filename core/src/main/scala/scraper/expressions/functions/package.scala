package scraper.expressions

package object functions {
  def lit(value: Any): Literal = Literal(value)

  def not(predicate: Expression): Expression = Not(predicate)

  def coalesce(first: Expression, second: Expression, rest: Expression*): Coalesce =
    Coalesce(Seq(first, second) ++ rest)

  def rand(seed: Int): Rand = Rand(seed)
}
