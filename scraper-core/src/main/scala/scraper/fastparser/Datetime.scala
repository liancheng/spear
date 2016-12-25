package scraper.fastparser

import fastparse.all._

// SQL06 section 5.3
object Datetime {
  import Keyword._
  import Numeric._
  import Symbol._
  import WhitespaceApi._

  private val datetimeValue: P0 =
    unsignedInteger opaque "datetime-value"

  val yearsValue: P0 =
    datetimeValue opaque "years-value"

  val monthsValue: P0 =
    datetimeValue opaque "months-value"

  val daysValue: P0 =
    datetimeValue opaque "days-value"

  private val unquotedDateString: P0 =
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
    sign ~~ hoursValue ~~ ":" ~~ minutesValue opaque "timezone-interval"

  private val unquotedTimeString: P0 =
    timeValue ~~ timezoneInterval.? opaque "unquoted-time-string"

  private val dateString: P0 =
    `'` ~~ unquotedDateString ~~ `'` opaque "date-string"

  private val dateLiteral: P0 =
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
