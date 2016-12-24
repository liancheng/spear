package scraper.fastparser

import fastparse.all._

// SQL06 section 5.3
object Datetime {
  import Keyword._
  import Numeric._
  import Symbol._

  private val datetimeValue: P0 = unsignedInteger

  val yearsValue: P0 = datetimeValue

  val monthsValue: P0 = datetimeValue

  val daysValue: P0 = datetimeValue

  private val unquotedDateString: P0 = yearsValue ~ "-" ~ monthsValue ~ "-" ~ daysValue

  val hoursValue: P0 = datetimeValue

  val minutesValue: P0 = datetimeValue

  val secondsValue: P0 = datetimeValue

  private val unquotedTimeString: P0 = {
    val timeValue: P0 = hoursValue ~ ":" ~ minutesValue ~ ":" ~ secondsValue
    val timeZoneInterval: P0 = sign ~ hoursValue ~ ":" ~ minutesValue
    timeValue ~ timeZoneInterval.?
  }

  val datetimeLiteral: P0 = {
    val dateString: P0 = `'` ~ unquotedDateString ~ `'`
    val dateLiteral: P0 = DATE ~ dateString

    val timeString: P0 = `'` ~ unquotedTimeString ~ `'`
    val timeLiteral: P0 = TIME ~ timeString

    val unquotedTimestampString: P0 = unquotedDateString ~ " " ~ unquotedTimeString
    val timestampString: P0 = `'` ~ unquotedTimestampString ~ `'`
    val timestampLiteral: P0 = TIMESTAMP ~ timestampString

    dateLiteral | timeLiteral | timestampLiteral
  }
}
