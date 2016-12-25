package scraper.fastparser

import fastparse.all._

object Symbol {
  val `'`: P0 = "'" opaque "quote"
  val `"`: P0 = "\"" opaque "double-quote"
  val `""`: P0 = "\"\"" opaque "double-double-quote"
}
