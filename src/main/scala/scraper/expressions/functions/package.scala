package scraper.expressions

package object functions {
  def lit(value: Any): Literal = Literal(value)

  def not(predicate: Predicate): Predicate = Not(predicate)
}
