package scraper

import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.types.TestUtils

class LocalContextSuite extends LoggingFunSuite with TestUtils {
  private val context = new LocalContext

  test("local data") {
    val data = Seq(1 -> "a", 2 -> "b")
    checkDataFrame(
      context lift data rename ("i", "s") where 'i =/= lit(1: Byte) + 1 select ('s, 'i),
      Row("a", 1)
    )
  }

  test("single row relation") {
    checkDataFrame(context select (1 as 'a) select 'a, Row(1))
  }
}
