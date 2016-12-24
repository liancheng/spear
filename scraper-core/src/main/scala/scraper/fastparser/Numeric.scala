package scraper.fastparser

import fastparse.all._

// SQL06 section 5.3
object Numeric {
  val sign: P0 = "+" | "-"

  val unsignedInteger: P0 = {
    val digit: P0 = CharIn('0' to '9')
    digit rep 1
  }

  val signedNumericLiteral: P0 = {
    val unsignedNumericLiteral: P0 = {
      val exactNumericLiteral: P0 = (
        unsignedInteger ~ ("." ~ unsignedInteger.?).?
        | "." ~ unsignedInteger
      )

      val approximateNumericLiteral: P0 = {
        val mantissa: P0 = exactNumericLiteral
        val signedInteger: P0 = sign ~ unsignedInteger
        val exponent: P0 = signedInteger
        mantissa ~ "E" ~ exponent
      }

      exactNumericLiteral | approximateNumericLiteral
    }

    sign ~ unsignedNumericLiteral
  }
}
