package scraper

import scraper.Context._
import scraper.LocalContextSuite.Person
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.types.TestUtils

class LocalContextSuite extends LoggingFunSuite with TestUtils {
  private implicit val context = new LocalContext

  test("local data") {
    val data = Seq(1 -> "a", 2 -> "b")
    checkDataFrame(
      context lift data rename ("i", "s") where 'i =/= lit(1: Byte) + 1 select ('s, 'i),
      Row("a", 1)
    )
  }

  test("single row relation") {
    checkDataFrame(context single (1 as 'a) select 'a, Row(1))
  }

  test("query string") {
    checkDataFrame("SELECT 1 AS a".q, Row(1))
  }

  private val people = context lift Seq(
    Person("Alice", 20),
    Person("Bob", 21),
    Person("Chris", 22)
  )

  test("mixed") {
    people filter 'age =/= 21 registerAsTable "people"

    checkDataFrame("SELECT name FROM people".q, Seq(
      Row("Alice"),
      Row("Chris")
    ))

    (people filter 'age =/= 21 select ('name, 'age)).queryExecution.analyzedPlan

    checkDataFrame("SELECT * FROM people".q, Seq(
      Row("Alice", 20),
      Row("Chris", 22)
    ))
  }

  test("resolution") {
    val df = context range 10 select ('id + 1 as 'x) where 'x > 5
    assert(df.queryExecution.analyzedPlan.strictlyTyped)
  }
}

object LocalContextSuite {
  case class Person(name: String, age: Int)
}
