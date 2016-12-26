package scraper.fastparser

import fastparse.all._

object SymbolParser {
  val `'`: P0 = "'" opaque "quote"
  val `''`: P0 = "'" opaque "quote-symbol"
  val `"`: P0 = "\"" opaque "double-quote"
  val `""`: P0 = "\"\"" opaque "double-quote-symbol"
  val `*`: P0 = "*" opaque "asterisk"
}
