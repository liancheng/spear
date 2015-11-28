package scraper.expressions

package object functions {
  def lit(value: Any): Literal = Literal(value)

  def not(predicate: Expression): Expression = Not(predicate)
}
