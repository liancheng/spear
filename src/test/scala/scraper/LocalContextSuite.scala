package scraper

import scraper.LocalContextSuite.Person
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

  test("query string") {
    checkDataFrame(context q "SELECT 1 AS a", Row(1))
  }

  test("mixed") {
    context lift Seq(
      Person("Alice", 20),
      Person("Bob", 21),
      Person("Chris", 22)
    ) filter 'age =/= 21 registerAsTable "people"

    checkDataFrame(context q "SELECT name FROM people", Seq(
      Row("Alice"),
      Row("Chris")
    ))
  }
}

object LocalContextSuite {
  case class Person(name: String, age: Int)
}
