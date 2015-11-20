package scraper.expressions

package object functions {
  def lit(value: Any): LiteralExpression = value match {
    case b: Boolean => LogicalLiteral(b)
    case _          => Literal(value)
  }

  def not(predicate: Predicate): Predicate = Not(predicate)
}
