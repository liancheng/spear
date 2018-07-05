package spear.parsers

import fastparse.all.{parserApi, AnyChar, CharPred, LiteralStr, P0}

// SQL06 section 5.2
object SeparatorParser {
  private val EoF = '\u001a'

  val whitespace: P0 = CharPred { ch => ch <= ' ' && ch != EoF } opaque "whitespace"

  private val comment: P0 = (
    "--" ~ (!"\n" ~ AnyChar).rep
    | "/*" ~ (!"*" ~ AnyChar).rep ~ "*/"
    opaque "comment"
  )

  val separator: P0 = comment | whitespace opaque "separator"
}
