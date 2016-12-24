package scraper.fastparser

import fastparse.all._

// SQL06 section 5.3
object Interval {
  import Datetime._
  import Keyword._
  import Numeric._
  import Symbol._

  val intervalLiteral: P0 = {
    val intervalString: P0 = {
      val dayTimeInterval: P0 = daysValue ~ (
        " " ~ hoursValue ~ (":" ~ minutesValue ~ (":" ~ secondsValue).?).?
      ).?

      val timeInterval: P0 = (
        hoursValue ~ (":" ~ minutesValue ~ (":" ~ secondsValue).?).?
        | minutesValue ~ (":" ~ secondsValue).?
        | secondsValue
      )

      val yearMonthLiteral: P0 = yearsValue ~ ("-" ~ monthsValue).? | monthsValue
      val dayTimeLiteral: P0 = dayTimeInterval | timeInterval
      val unquotedIntervalString: P0 = sign.? ~ (yearMonthLiteral | dayTimeLiteral)
      `'` ~ unquotedIntervalString ~ `'`
    }

    val intervalQualifier: P0 = {
      val nonSecondPrimaryDatetimeField: P0 = YEAR | MONTH | DAY | HOUR | MINUTE
      val intervalLeadingFieldPrecision: P0 = unsignedInteger

      val startField: P0 = (
        nonSecondPrimaryDatetimeField
        ~ ("(" ~ intervalLeadingFieldPrecision ~ ")").?
      )

      val endField: P0 = (
        nonSecondPrimaryDatetimeField
        | SECOND ~ ("(" ~ intervalLeadingFieldPrecision ~ ")").?
      )

      val intervalFractionalSecondsPrecision: P0 = unsignedInteger

      val singleDatetimeField: P0 = (
        nonSecondPrimaryDatetimeField ~ ("(" ~ intervalLeadingFieldPrecision ~ ")").?
        | SECOND ~ (
          "(" ~ intervalLeadingFieldPrecision ~ ("," ~ intervalFractionalSecondsPrecision).? ~ ")"
        ).?
      )

      startField ~ TO ~ endField | singleDatetimeField
    }

    INTERVAL ~ sign.? ~ intervalString ~ intervalQualifier
  }
}
