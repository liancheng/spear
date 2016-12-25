package scraper.fastparser

import fastparse.all._

// SQL06 section 5.3
object Numeric {
  val sign: P0 =
    "+" | "-" opaque "sign"

  private val digit: P0 =
    CharIn('0' to '9') opaque "digit"

  val unsignedInteger: P0 =
    digit rep 1 opaque "unsigned-integer"

  val exactNumericLiteral: P0 = (
    unsignedInteger ~ ("." ~ unsignedInteger.?).?
    | "." ~ unsignedInteger
  ) opaque "exact-numeric-literal"

  val mantissa: P0 =
    exactNumericLiteral opaque "exact-numeric-literal"

  val signedInteger: P0 =
    sign ~ unsignedInteger opaque "signed-integer"

  val exponent: P0 =
    signedInteger opaque "exponent"

  val approximateNumericLiteral: P0 =
    mantissa ~ "E" ~ exponent opaque "approximate-numeric-literal"

  val unsignedNumericLiteral: P0 =
    exactNumericLiteral | approximateNumericLiteral opaque "unsigned-numeric-literal"

  val signedNumericLiteral: P0 =
    sign ~ unsignedNumericLiteral opaque "signed-numeric-literal"
}
