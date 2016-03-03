package scraper.expressions

import scraper.config.Settings
import scraper.parser.Parser

package object functions {
  def col(name: String): UnresolvedAttribute = new Parser(Settings.empty).parseAttribute(name)

  def lit(value: Any): Literal = Literal(value)

  def not(predicate: Expression): Expression = Not(predicate)

  def coalesce(first: Expression, second: Expression, rest: Expression*): Coalesce =
    Coalesce(Seq(first, second) ++ rest)

  def rand(seed: Int): Rand = Rand(seed)
}
