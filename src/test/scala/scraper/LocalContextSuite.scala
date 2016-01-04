package scraper

import scraper.Context._
import scraper.LocalContextSuite.Person
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.types.{StringType, TestUtils}

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

  test("join") {
    val left = context range 2
    val right = context range 2 select ('id + 1 cast StringType as "str")

    checkDataFrame(
      left join right,
      Seq(Row(0, "1"), Row(0, "2"), Row(1, "1"), Row(1, "2"))
    )

    checkDataFrame(
      left join right on 'id > 0,
      Seq(Row(1, "1"), Row(1, "2"))
    )
  }

  ignore("aggregate") {
    val df = context.lift(Seq("a" -> -1, "a" -> 1, "a" -> 2, "b" -> 4, "b" -> 5), "i", "j")

    checkDataFrame(
      df groupBy 'i agg ('i, count) orderBy 'i,
      Seq(Row("a", 3), Row("b", 2))
    )

    checkDataFrame(
      df groupBy 'i agg ('i, sum('j)) orderBy 'i,
      Seq(Row("a", 2.0), Row("b", 9.0))
    )

    checkDataFrame(
      df groupBy 'i agg ('i, max('j)) orderBy 'i,
      Seq(Row("a", 2.0), Row("b", 5.0))
    )

    checkDataFrame(
      df groupBy 'i agg (min('j), 'i) orderBy 'i,
      Seq(Row(-1.0, "a"), Row(4.0, "b"))
    )

    checkDataFrame(
      df groupBy 'i === "a" agg (min('j + 1) - 2, ('i === "a").as("bool")) orderBy 'bool,
      Seq(Row(3.0, false), Row(-2.0, true))
    )

    checkDataFrame(
      df agg (min('j) + 1, max('j) * 2),
      Seq(Row(0.0, 10.0))
    )

    checkDataFrame(
      df groupBy 'i agg (count, sum('j), max('j), min('j), ('i === "a").as("bool")) orderBy 'bool,
      Seq(Row(2, 9.0, 5.0, 4.0, false), Row(3, 2.0, 2.0, -1.0, true))
    )
  }

  test("sort") {
    val df = context.lift(Seq("a" -> 3, "b" -> 1, "f" -> 2, "d" -> 4, "c" -> 5), "i", "j")

    checkDataFrame(
      df orderBy 'i,
      Seq(Row("a", 3), Row("b", 1), Row("c", 5), Row("d", 4), Row("f", 2))
    )

    checkDataFrame(
      df orderBy 'j,
      Seq(Row("b", 1), Row("f", 2), Row("a", 3), Row("d", 4), Row("c", 5))
    )
  }
}

object LocalContextSuite {
  case class Person(name: String, age: Int)
}
