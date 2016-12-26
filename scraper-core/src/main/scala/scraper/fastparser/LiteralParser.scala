package scraper.fastparser

import fastparse.all._

import scraper.expressions.Literal
import scraper.expressions.Literal.{False, True}

// SQL06 section 5.3
object StringParser {
  import IdentifierParser._
  import SeparatorParser._
  import SymbolParser._
  import WhitespaceApi._

  private val nonquoteCharacter: P[Char] =
    !`'` ~ AnyChar.char opaque "nonquote-character"

  private val characterRepresentation: P[Char] =
    nonquoteCharacter | `''`.char opaque "character-representation"

  val characterStringLiteral: P[Literal] = (
    `'` ~~ characterRepresentation.repX ~~ `'`
    repX (min = 1, sep = separator)
    map (_.mkString: Literal)
    opaque "character-string-literal"
  )

  val unicodeCharacterStringLiteral: P[Literal] = (
    "U&"
    ~~ (`'` ~~ characterRepresentation ~~ `'`).rep(min = 1, sep = separator)
    ~ unicodeEscapeSpecifier map {
      case (body, uescape) =>
        Literal(parseUnicodeRepresentations(body.mkString, uescape))
    } opaque "unicode-character-string-literal"
  )
}

// SQL06 section 5.3
object NumericParser {
  import WhitespaceApi._

  val sign: P[Int] =
    P("+").map(_ => 1) | P("-").map(_ => -1) opaque "sign"

  private val digit: P0 =
    CharIn('0' to '9') opaque "digit"

  val unsignedInteger: P0 =
    digit rep 1 opaque "unsigned-integer"

  private val exactNumeric: P[BigDecimal] = (
    unsignedInteger ~ ("." ~ unsignedInteger.?).?
    | "." ~ unsignedInteger.X
  ).! map (BigDecimal(_)) opaque "exact-numeric"

  private val mantissa: P0 =
    exactNumeric.X opaque "mantissa"

  private val signedInteger: P[BigInt] =
    (sign ~ unsignedInteger).! map (BigInt(_, 10)) opaque "signed-integer"

  private val exponent: P[BigInt] =
    signedInteger opaque "exponent"

  private val approximateNumeric: P[BigDecimal] =
    (mantissa ~ "E" ~ exponent.X).! map (BigDecimal(_)) opaque "approximate-numeric"

  private val unsignedNumeric: P[BigDecimal] =
    exactNumeric | approximateNumeric opaque "unsigned-numeric"

  private val signedNumeric: P[BigDecimal] =
    sign ~ unsignedNumeric map {
      case (signum, unsigned) => signum * unsigned
    } opaque "signed-numeric"

  private def toLiteral(d: BigDecimal): Literal = d match {
    case _ if d.isValidByte    => Literal(d.toByte)
    case _ if d.isValidShort   => Literal(d.toShort)
    case _ if d.isValidInt     => Literal(d.toInt)
    case _ if d.isValidLong    => Literal(d.toLong)
    case _ if d.isBinaryFloat  => Literal(d.toFloat)
    case _ if d.isBinaryDouble => Literal(d.toDouble)
  }

  val unsignedNumericLiteral: P[Literal] =
    unsignedNumeric map toLiteral opaque "unsigned-numeric-literal"

  val signedNumericLiteral: P[Literal] =
    signedNumeric map toLiteral opaque "unsigned-numeric-literal"
}

// SQL06 section 5.3
object DatetimeParser {
  import KeywordParser._
  import NumericParser._
  import SymbolParser._
  import WhitespaceApi._

  private val datetimeValue: P0 =
    unsignedInteger opaque "datetime-value"

  val yearsValue: P0 =
    datetimeValue opaque "years-value"

  val monthsValue: P0 =
    datetimeValue opaque "months-value"

  val daysValue: P0 =
    datetimeValue opaque "days-value"

  val unquotedDateString: P0 =
    yearsValue ~~ "-" ~~ monthsValue ~~ "-" ~~ daysValue opaque "unquoted-date-string"

  val hoursValue: P0 =
    datetimeValue opaque "hours-value"

  val minutesValue: P0 =
    datetimeValue opaque "minutes-value"

  val secondsValue: P0 =
    datetimeValue opaque "seconds-value"

  private val timeValue: P0 =
    hoursValue ~~ ":" ~~ minutesValue ~~ ":" ~~ secondsValue opaque "time-value"

  private val timezoneInterval: P0 =
    sign.X ~~ hoursValue ~~ ":" ~~ minutesValue opaque "timezone-interval"

  val unquotedTimeString: P0 =
    timeValue ~~ timezoneInterval.? opaque "unquoted-time-string"

  private val dateString: P0 =
    `'` ~~ unquotedDateString ~~ `'` opaque "date-string"

  val dateLiteral: P0 =
    DATE ~ dateString opaque "date-literal"

  private val timeString: P0 =
    `'` ~~ unquotedTimeString ~~ `'` opaque "time-string"

  private val timeLiteral: P0 =
    TIME ~ timeString opaque "time-literal"

  private val unquotedTimestampString: P0 =
    unquotedDateString ~~ " " ~~ unquotedTimeString opaque "unquoted-timestamp-string"

  private val timestampString: P0 =
    `'` ~~ unquotedTimestampString ~~ `'` opaque "timestamp-string"

  private val timestampLiteral: P0 =
    TIMESTAMP ~ timestampString opaque "timestamp-literal"

  val datetimeLiteral: P0 =
    dateLiteral | timeLiteral | timestampLiteral opaque "datetime-literal"
}

// SQL06 section 5.3
object IntervalParser {
  import DatetimeParser._
  import KeywordParser._
  import NumericParser._
  import SymbolParser._
  import WhitespaceApi._

  private val secondInterval: P0 =
    secondsValue opaque "second-interval"

  private val minuteSecondInterval: P0 =
    minutesValue ~~ (":" ~~ secondInterval).? opaque "minute-second-interval"

  private val hourMinuteSecondInterval: P0 =
    hoursValue ~~ (":" ~~ minuteSecondInterval).? opaque "hour-minute-second-interval"

  private val dayTimeInterval: P0 =
    daysValue ~~ (" " ~~ hourMinuteSecondInterval.?).? opaque "day-time-interval"

  private val timeInterval: P0 =
    hourMinuteSecondInterval | minuteSecondInterval | secondInterval opaque "time-interval"

  private val yearMonthLiteral: P0 =
    yearsValue ~~ ("-" ~~ monthsValue).? | monthsValue opaque "year-month-literal"

  private val dayTimeLiteral: P0 =
    dayTimeInterval | timeInterval opaque "day-time-literal"

  private val unquotedIntervalString: P0 =
    sign.X.? ~~ (yearMonthLiteral | dayTimeLiteral) opaque "unquoted-interval-string"

  private val intervalString: P0 =
    `'` ~~ unquotedIntervalString ~~ `'` opaque "interval-string"

  private val nonSecondPrimaryDatetimeField: P0 =
    YEAR | MONTH | DAY | HOUR | MINUTE opaque "non-second-primary-datetime-field"

  private val intervalLeadingFieldPrecision: P0 =
    unsignedInteger opaque "interval-leading-field-precision"

  private val startField: P0 = (
    nonSecondPrimaryDatetimeField ~ ("(" ~ intervalLeadingFieldPrecision ~ ")").?
  ) opaque "start-field"

  private val endField: P0 = (
    nonSecondPrimaryDatetimeField
    | SECOND ~ ("(" ~ intervalLeadingFieldPrecision ~ ")").?
  ) opaque "end-field"

  private val intervalFractionalSecondsPrecision: P0 =
    unsignedInteger opaque "interval-fractional-seconds-precision"

  private val singleDatetimeField: P0 = (
    nonSecondPrimaryDatetimeField ~ ("(" ~ intervalLeadingFieldPrecision ~ ")").?
    | SECOND ~ (
      "(" ~ intervalLeadingFieldPrecision ~~ ("," ~~ intervalFractionalSecondsPrecision).? ~ ")"
    ).?
  ) opaque "single-datetime-field"

  private val intervalQualifier: P0 =
    startField ~ TO ~ endField | singleDatetimeField opaque "interval-qualifier"

  val intervalLiteral: P0 =
    INTERVAL ~ sign.X.? ~~ intervalString ~ intervalQualifier opaque "interval-literal"
}

// SQL06 section 5.3
object LiteralParser {
  import KeywordParser._
  import NumericParser._
  import StringParser._
  import WhitespaceApi._

  private val booleanLiteral: P[Literal] = (
    TRUE.map(_ => True)
    | FALSE.map(_ => False)
  ) opaque "boolean-literal"

  private val generalLiteral: P[Literal] = (
    characterStringLiteral
    | unicodeCharacterStringLiteral
    | booleanLiteral
  ) opaque "general-literal"

  val literal: P[Literal] = (
    signedNumericLiteral
    | generalLiteral
  ) opaque "literal"

  val unsignedLiteral: P[Literal] = (
    unsignedNumericLiteral
    | generalLiteral
  ) opaque "unsigned-literal"
}
