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
    val right = context.range(2).select(('id + 1).cast(StringType).as("str"))

    checkDataFrame(
      left.join(right),
      Seq(Row(0, "1"), Row(0, "2"), Row(1, "1"), Row(1, "2"))
    )

    checkDataFrame(
      left.join(right, Some('id > 0)),
      Seq(Row(1, "1"), Row(1, "2"))
    )
  }

  test("aggregate") {
    import GroupedData._

    val df = context.lift(Seq("a" -> -1, "a" -> 1, "a" -> 2, "b" -> 4, "b" -> 5), "i", "j")

    checkDataFrame(
      df.groupBy('i).agg(count()),
      Seq(Row(3), Row(2))
    )

    checkDataFrame(
      df.groupBy('i).agg(sum('j)),
      Seq(Row(2.0), Row(9.0))
    )

    checkDataFrame(
      df.groupBy('i).agg(max('j)),
      Seq(Row(2.0), Row(5.0))
    )

    checkDataFrame(
      df.groupBy('i).agg(min('j)),
      Seq(Row(-1.0), Row(4.0))
    )
  }
}

object LocalContextSuite {
  case class Person(name: String, age: Int)
}
