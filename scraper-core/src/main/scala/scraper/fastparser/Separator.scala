package scraper.fastparser

import fastparse.all._

// SQL06 section 5.2
object Separator {
  val whitespace: P0 = {
    val EoF = '\u001a'
    CharPred { ch => ch <= ' ' && ch != EoF }
  }

  val separator: P0 = {
    val comment: P0 = (
      "--" ~ (!"\n").rep
      | "/*" ~ (!"*").rep ~ "*/"
    )

    comment | whitespace
  }
}
