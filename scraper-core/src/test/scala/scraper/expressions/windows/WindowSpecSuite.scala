package scraper.expressions.windows

import scala.util.Success

import scraper.LoggingFunSuite
import scraper.expressions._
import scraper.types.{DoubleType, IntType, StringType}
import scraper.utils._

class WindowSpecSuite extends LoggingFunSuite {
  test("full window spec") {
    val expected = Success(
      """PARTITION BY a, b
        |ORDER BY c ASC NULLS FIRST
        |ROWS BETWEEN
        |UNBOUNDED PRECEDING
        |AND
        |UNBOUNDED FOLLOWING
        |""".oneLine.trim
    )

    assertResult(expected) {
      Window
        .partitionBy(a, b)
        .orderBy(c.asc.nullsFirst)
        .rowsBetween(UnboundedPreceding, UnboundedFollowing)
        .sql
    }
  }

  test("window spec without partition spec") {
    val expected = Success(
      """ORDER BY c ASC NULLS LAST
        |ROWS BETWEEN
        |CURRENT ROW
        |AND
        |10 FOLLOWING
        |""".oneLine.trim
    )

    assertResult(expected) {
      Window
        .orderBy(c.asc)
        .rowsBetween(CurrentRow, Following(10))
        .sql
    }
  }

  test("window spec without order spec") {
    val expected = Success(
      """PARTITION BY a, b
        |ROWS BETWEEN
        |CURRENT ROW
        |AND
        |10 FOLLOWING
      """.oneLine
    )

    assertResult(expected) {
      Window
        .partitionBy(a, b)
        .rowsBetween(CurrentRow, Following(10))
        .sql
    }
  }

  test("window spec without partition spec or order spec") {
    val expected = Success(
      """ROWS BETWEEN
        |CURRENT ROW
        |AND
        |10 FOLLOWING
        |""".oneLine.trim
    )

    assertResult(expected) {
      Window
        .rowsBetween(CurrentRow, Following(10))
        .sql
    }
  }

  private val (a, b, c) = ('a of IntType, 'b of StringType, 'c of DoubleType)
}
