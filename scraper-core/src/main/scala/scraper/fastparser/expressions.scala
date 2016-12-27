package scraper.fastparser

import fastparse.all._

import scraper.expressions._
import scraper.expressions.Literal.{False, True}
import scraper.plans.logical.LogicalPlan

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

  private val quotedCharacterRepresentations: P[String] = (
    `'` ~~ characterRepresentation.repX ~~ `'`
    map { _.mkString }
    opaque "quoted-character-representations"
  )

  private val quotedCharacterRepresentationsList: P[String] = (
    quotedCharacterRepresentations
    rep (min = 1, sep = separator)
    map { _.mkString }
    opaque "quoted-character-representations-list"
  )

  val characterStringLiteral: P[Literal] =
    quotedCharacterRepresentationsList map Literal.apply opaque "character-string-literal"

  val unicodeCharacterStringLiteral: P[Literal] = (
    "U&" ~~ quotedCharacterRepresentationsList ~ unicodeEscapeSpecifier
    map (parseUnicodeRepresentations _).tupled
    map Literal.apply
    opaque "unicode-character-string-literal"
  )
}

// SQL06 section 5.3
object NumericParser {
  import WhitespaceApi._

  val sign: P[Int] =
    P("+").map { _ => 1 } | P("-").map { _ => -1 } opaque "sign"

  val maybeSign: P[Int] =
    sign.? map { _ getOrElse 1 } opaque "maybe-sign"

  private val digit: P0 =
    CharIn('0' to '9') opaque "digit"

  val unsignedInteger: P0 =
    digit rep 1 opaque "unsigned-integer"

  private val exactNumeric: P[BigDecimal] = (
    unsignedInteger ~ ("." ~ unsignedInteger.?).?
    | "." ~ unsignedInteger
  ).! map { BigDecimal(_) } opaque "exact-numeric"

  private val mantissa: P0 =
    exactNumeric.drop opaque "mantissa"

  private val signedInteger: P[BigInt] = (
    (sign.? ~ unsignedInteger).!
    map { BigInt(_, 10) }
    opaque "signed-integer"
  )

  private val exponent: P[BigInt] =
    signedInteger opaque "exponent"

  private val approximateNumeric: P[BigDecimal] = (
    (mantissa ~ "E" ~ exponent.drop).!
    map { BigDecimal(_) }
    opaque "approximate-numeric"
  )

  private val unsignedNumeric: P[BigDecimal] =
    exactNumeric | approximateNumeric opaque "unsigned-numeric"

  private val signedNumeric: P[BigDecimal] = (
    maybeSign ~ unsignedNumeric
    map { case (signum, unsigned) => signum * unsigned }
    opaque "signed-numeric"
  )

  private def toLiteral(d: BigDecimal): Literal = d match {
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
    sign.drop ~~ hoursValue ~~ ":" ~~ minutesValue opaque "timezone-interval"

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
    sign.drop.? ~~ (yearMonthLiteral | dayTimeLiteral) opaque "unquoted-interval-string"

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
    INTERVAL ~ sign.drop.? ~~ intervalString ~ intervalQualifier opaque "interval-literal"
}

// SQL06 section 5.3
object LiteralParser {
  import KeywordParser._
  import NumericParser._
  import StringParser._
  import WhitespaceApi._

  val booleanLiteral: P[Literal] = (
    TRUE.map { _ => True }
    | FALSE.map { _ => False }
    opaque "boolean-literal"
  )

  private val generalLiteral: P[Literal] = (
    characterStringLiteral
    | unicodeCharacterStringLiteral
    | booleanLiteral
    opaque "general-literal"
  )

  val literal: P[Literal] = (
    signedNumericLiteral
    | generalLiteral
    opaque "literal"
  )

  val unsignedLiteral: P[Literal] = (
    unsignedNumericLiteral
    | generalLiteral
    opaque "unsigned-literal"
  )
}

// SQL06 section 6.3
object ValueExpressionPrimaryParser {
  import ColumnReferenceParser._
  import ValueExpressionParser._
  import ValueSpecificationParser._
  import WhitespaceApi._

  private val parenthesizedValueExpressionPrimary: P[Expression] =
    "(" ~ valueExpression ~ ")" opaque "parenthesized-value-expression-primary"

  lazy val nonparenthesizedValueExpressionPrimary: P[Expression] = (
    unsignedValueSpecification
    | columnReference
    opaque "nonparenthesized-value-expression-primary"
  )

  lazy val valueExpressionPrimary: P[Expression] = (
    parenthesizedValueExpressionPrimary
    | nonparenthesizedValueExpressionPrimary
    opaque "value-expression-primary"
  )
}

// SQL06 section 6.4
object ValueSpecificationParser {
  import LiteralParser._
  import WhitespaceApi._

  lazy val unsignedValueSpecification: P[Literal] =
    unsignedLiteral opaque "unsigned-value-specification"
}

// SQL06 section 6.7
object ColumnReferenceParser {
  import IdentifierChainParser._
  import scraper.expressions._

  lazy val columnReference: P[Attribute] =
    basicIdentifierChain map {
      case Seq(qualifier, column) => column of qualifier
      case Seq(column)            => column: Attribute
    } opaque "column-reference"
}

// SQL06 section 6.25
object ValueExpressionParser {
  import BooleanValueExpressionParser._
  import NumericValueExpressionParser._
  import WhitespaceApi._

  lazy val commonValueExpression: P[Expression] =
    numericValueExpression opaque "common-value-expression"

  lazy val valueExpression: P[Expression] = (
    booleanValueExpression
    | commonValueExpression
    opaque "value-expression"
  )
}

// SQL06 section 6.26
object NumericValueExpressionParser {
  import NumericParser._
  import ValueExpressionPrimaryParser._
  import WhitespaceApi._

  private val numericPrimary: P[Expression] =
    valueExpressionPrimary opaque "numeric-primary"

  private val factor: P[Expression] = (
    (maybeSign map Literal.apply) ~ numericPrimary
    map { _.swap }
    map Multiply.tupled
    opaque "factor"
  )

  private val term: P[Expression] = (
    factor
    | (term ~ "*" ~ factor map Multiply.tupled)
    | (term ~ "/" ~ factor map Divide.tupled)
    opaque "term"
  )

  lazy val numericValueExpression: P[Expression] = P(
    term
      | (numericValueExpression ~ "+" ~ term map Plus.tupled)
      | (numericValueExpression ~ "-" ~ term map Minus.tupled)
      opaque "numeric-value-expression"
  )
}

// SQL06 section 6.34
object BooleanValueExpressionParser {
  import KeywordParser._
  import LiteralParser._
  import PredicateParser._
  import ValueExpressionPrimaryParser._
  import WhitespaceApi._

  private val parenthesizedBooleanValueExpression: P[Expression] = (
    "(" ~ booleanValueExpression ~ ")"
    opaque "parenthesized-boolean-value-expression"
  )

  lazy val booleanPredicand: P[Expression] = (
    parenthesizedBooleanValueExpression
    | nonparenthesizedValueExpressionPrimary
    opaque "boolean-predicand"
  )

  private val booleanPrimary: P[Expression] =
    predicate | booleanPredicand opaque "boolean-primary"

  private val truthValue: P[Literal] =
    booleanLiteral opaque "truth-value"

  private val booleanTestSuffix: P[Expression => Expression] = {
    IS ~ NOT.!.? ~ truthValue map {
      case (Some(_), bool) if bool == True  => Not
      case (Some(_), bool) if bool == False => identity[Expression] _
      case (None, bool) if bool == True     => identity[Expression] _
      case (None, bool) if bool == False    => Not
    }
  }.? map {
    _ getOrElse identity[Expression] _
  } opaque "boolean-test-suffix"

  private val booleanTest: P[Expression] = (
    booleanPrimary ~ booleanTestSuffix
    map { case (bool, f) => f(bool) }
    opaque "boolean-test"
  )

  private val booleanFactor: P[Expression] = (
    (NOT ~ booleanTest map Not)
    | booleanTest
    opaque "boolean-factor"
  )

  private val booleanTerm: P[Expression] = (
    booleanFactor
    | (booleanTerm ~ AND ~ booleanFactor map And.tupled)
    opaque "boolean-term"
  )

  lazy val booleanValueExpression: P[Expression] = P(
    booleanTerm
      | (booleanValueExpression ~ OR ~ booleanTerm map Or.tupled)
      opaque "boolean-value-expression"
  )
}

// SQL06 section 7.1
object RowValueConstructorParser {
  import BooleanValueExpressionParser._
  import ValueExpressionParser._
  import WhitespaceApi._

  lazy val rowValueConstructorPredicand: P[Expression] = (
    commonValueExpression
    | booleanPredicand
    opaque "row-value-constructor-predicand"
  )
}

// SQL06 section 7.2
object RowValueExpressionParser {
  import RowValueConstructorParser._
  import ValueExpressionPrimaryParser._
  import WhitespaceApi._

  private val rowValueSpecialCase: P[Expression] =
    nonparenthesizedValueExpressionPrimary opaque "row-value-special-case"

  lazy val rowValuePredicand: P[Expression] = (
    rowValueSpecialCase
    | rowValueConstructorPredicand
    opaque "row-value-predicand"
  )
}

// SQL06 section 8.1
object PredicateParser {
  import RowValueExpressionParser._
  import WhitespaceApi._

  lazy val comparisonPredicate: P[Expression] = (
    (rowValuePredicand ~ "=" ~ rowValuePredicand map Eq.tupled)
    | (rowValuePredicand ~ "<>" ~ rowValuePredicand map NotEq.tupled)
    | (rowValuePredicand ~ "<" ~ rowValuePredicand map Lt.tupled)
    | (rowValuePredicand ~ ">" ~ rowValuePredicand map Gt.tupled)
    | (rowValuePredicand ~ "<=" ~ rowValuePredicand map LtEq.tupled)
    | (rowValuePredicand ~ ">=" ~ rowValuePredicand map GtEq.tupled)
    opaque "comparison-predicate"
  )

  lazy val predicate: P[Expression] =
    comparisonPredicate opaque "predicate"
}

// SQL06 section 10.9
object AggregateFunctionParser {
  import KeywordParser._
  import WhitespaceApi._

  lazy val setQuantifier: P[LogicalPlan => LogicalPlan] = (
    ALL.map { _ => identity[LogicalPlan] _ }
    | DISTINCT.map { _ => (_: LogicalPlan).distinct }
    opaque "setQuantifier"
  )
}
