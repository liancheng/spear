package scraper.fastparser

import fastparse.all._

// SQL06 section 5.3
object Interval {
  import Datetime._
  import Keyword._
  import Numeric._
  import Symbol._

  private val secondInterval: P0 =
    secondsValue opaque "second-interval"

  private val minuteSecondInterval: P0 =
    minutesValue ~ (":" ~ secondInterval).? opaque "minute-second-interval"

  private val hourMinuteSecondInterval: P0 =
    hoursValue ~ (":" ~ minuteSecondInterval).? opaque "hour-minute-second-interval"

  private val dayTimeInterval: P0 =
    daysValue ~ (" " ~ hourMinuteSecondInterval.?).? opaque "day-time-interval"

  private val timeInterval: P0 =
    hourMinuteSecondInterval | minuteSecondInterval | secondInterval opaque "time-interval"

  private val yearMonthLiteral: P0 =
    yearsValue ~ ("-" ~ monthsValue).? | monthsValue opaque "year-month-literal"

  private val dayTimeLiteral: P0 =
    dayTimeInterval | timeInterval opaque "day-time-literal"

  private val unquotedIntervalString: P0 =
    sign.? ~ (yearMonthLiteral | dayTimeLiteral) opaque "unquoted-interval-string"

  private val intervalString: P0 =
    `'` ~ unquotedIntervalString ~ `'` opaque "interval-string"

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
      "(" ~ intervalLeadingFieldPrecision ~ ("," ~ intervalFractionalSecondsPrecision).? ~ ")"
    ).?
  ) opaque "single-datetime-field"

  private val intervalQualifier: P0 =
    startField ~ TO ~ endField | singleDatetimeField opaque "interval-qualifier"

  val intervalLiteral: P0 =
    INTERVAL ~ sign.? ~ intervalString ~ intervalQualifier opaque "interval-literal"
}
