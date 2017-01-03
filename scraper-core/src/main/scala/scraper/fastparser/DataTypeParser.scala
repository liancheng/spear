package scraper.fastparser

import fastparse.all._

import scraper.types._

object DataTypeParser {
  import KeywordParser._
  import NameParser._
  import NumericParser._
  import WhitespaceApi._

  private val exactNumericType: P[DataType] = (
    (SMALLINT attach ShortType)
    | (INTEGER attach IntType)
    | (INT attach IntType)
    | (BIGINT attach LongType)
    opaque "exact-numeric-type"
  )

  private val approximateNumericType: P[DataType] = (
    (FLOAT attach FloatType)
    | (REAL attach DoubleType)
    | (DOUBLE attach DoubleType)
    opaque "approximate-numeric-type"
  )

  private val numericType: P[DataType] = (
    exactNumericType
    | approximateNumericType
    opaque "numeric-type"
  )

  private val booleanType: P[DataType] =
    BOOLEAN attach BooleanType opaque "boolean-type"

  private val timeFractionalSecondsPrecision: P[Int] =
    unsignedInteger.! map Integer.parseInt opaque "time-fractional-seconds-precision"

  private val timePrecision: P[Int] =
    timeFractionalSecondsPrecision opaque "time-precision"

  private val timestampPrecision: P[Int] =
    timeFractionalSecondsPrecision opaque "timestamp-precision"

  private val withOrWithoutTimeZone: P0 =
    (WITH | WITHOUT) ~ TIME ~ ZONE opaque "with-or-without-time-zone"

  private val timestampType: P0 = (
    TIMESTAMP
    ~ ("(" ~ timestampPrecision ~ ")").?.map { _ getOrElse 6 }
    ~ withOrWithoutTimeZone.?
    opaque "timestamp-type"
  ).drop

  private val timeType: P0 = (
    TIME
    ~ ("(" ~ timePrecision ~ ")").?.map { _ getOrElse 0 }
    ~ withOrWithoutTimeZone.?
    opaque "timeType"
  ).drop

  val datetimeType: P0 = (
    (DATE attach DateType).drop
    | timestampType
    | timeType
    opaque "datetime-type"
  )

  private val predefinedType: P0 = (
    numericType.drop
    | booleanType.drop
    | datetimeType
    opaque "predefined-type"
  )

  private val fieldDefinition: P0 =
    fieldName.drop ~ P(dataType) opaque "field-definition"

  private val rowTypeBody: P0 =
    "(" ~ fieldDefinition.rep(min = 1, sep = ",") ~ ")" opaque "row-type-body"

  private val rowType: P0 =
    ROW ~ rowTypeBody opaque "row-type"

  lazy val dataType: P0 =
    predefinedType | rowType.drop opaque "data-type"
}
