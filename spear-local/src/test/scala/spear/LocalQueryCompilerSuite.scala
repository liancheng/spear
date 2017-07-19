package spear

import spear.LocalQueryCompilerSuite.Person
import spear.exceptions.TableNotFoundException
import spear.expressions._
import spear.expressions.functions._
import spear.local.LocalQueryCompiler
import spear.types.StringType

class LocalQueryCompilerSuite extends LoggingFunSuite with TestUtils {
  private implicit val context = new Context(new LocalQueryCompiler)

  import context._

  test("local data") {
    val data = Seq(1 -> "a", 2 -> "b")
    checkDataFrame(
      lift(data) rename ("i", "s") filter 'i =/= lit(1: Byte) + 1 select ('s, 'i),
      Row("a", 1)
    )
  }

  test("range") {
    checkDataFrame(
      range(3),
      Row(0), Row(1), Row(2)
    )

    checkDataFrame(
      range(1, 4),
      Row(1), Row(2), Row(3)
    )

    checkDataFrame(
      range(1, 6, 2),
      Row(1), Row(3), Row(5)
    )
  }

  test("single row relation") {
    checkDataFrame(values(1 as 'a) select 'a, Row(1))
  }

  test("query string") {
    checkDataFrame(sql("SELECT 1 AS a"), Row(1))
  }

  test("table") {
    withTable('t) {
      range(2) asTable 't

      checkDataFrame(
        table('t),
        Row(0), Row(1)
      )
    }

    intercept[TableNotFoundException] {
      table("non-existed")
    }
  }

  private val people = lift(
    Person("Alice", 20),
    Person("Bob", 21),
    Person("Chris", 22)
  )

  test("mixed") {
    people filter 'age =/= 21 asTable 'people

    checkDataFrame(
      sql("SELECT name FROM people"),
      Row("Alice"), Row("Chris")
    )

    (people filter 'age =/= 21 select ('name, 'age)).query.analyzedPlan

    checkDataFrame(
      sql("SELECT * FROM people"),
      Row("Alice", 20), Row("Chris", 22)
    )
  }

  test("resolution") {
    val df = range(10) select ('id + 1 as 'x) filter 'x > 5
    checkStrictlyTyped(df.query.analyzedPlan)
  }

  test("join") {
    val left = range(2)
    val right = range(2) select ('id + 1 cast StringType as "str")

    checkDataFrame(
      left crossJoin right,
      Row(0, "1"), Row(0, "2"), Row(1, "1"), Row(1, "2")
    )

    checkDataFrame(
      left join right on 'id > 0,
      Row(1, "1"), Row(1, "2")
    )
  }

  test("sort") {
    val df = lift("a" -> 3, "b" -> 1, "f" -> 2, "d" -> 4, "c" -> 5) rename ("i", "j")

    checkDataFrame(
      df orderBy 'i,
      Row("a", 3), Row("b", 1), Row("c", 5), Row("d", 4), Row("f", 2)
    )

    checkDataFrame(
      df orderBy 'j,
      Row("b", 1), Row("f", 2), Row("a", 3), Row("d", 4), Row("c", 5)
    )
  }

  test("self-join") {
    val df = range(2)

    checkDataFrame(
      df subquery 'a crossJoin (df subquery 'b),
      Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1)
    )

    checkDataFrame(
      df subquery 'a join (df subquery 'b) on $"a.id" === $"b.id",
      Row(0, 0), Row(1, 1)
    )
  }

  test("filter over join") {
    val df = range(3)

    checkDataFrame(
      df subquery 'a join (df subquery 'b) on $"a.id" === $"b.id" filter $"b.id" > 0,
      Row(1, 1), Row(2, 2)
    )
  }

  test("sort over aggregation") {
    checkDataFrame(
      range(3) groupBy 'id agg 'id orderBy 'id,
      Row(0), Row(1), Row(2)
    )
  }

  test("aggregate constant expression") {
    checkDataFrame(
      range(3) agg count(1),
      Row(3)
    )
  }

  test("sum") {
    checkDataFrame(
      range(3) agg sum('id),
      Row(3)
    )
  }

  test("max") {
    checkDataFrame(
      range(3) agg max('id),
      Row(2)
    )
  }

  test("min") {
    checkDataFrame(
      range(3) agg min('id),
      Row(0)
    )
  }

  test("bool_and") {
    checkDataFrame(
      range(2) agg bool_and('id % 2 === 0),
      Row(false)
    )
  }

  test("bool_or") {
    checkDataFrame(
      range(2) agg bool_or('id % 2 === 0),
      Row(true)
    )
  }

  test("average") {
    checkDataFrame(
      range(2) agg avg('id),
      Row(0.5D)
    )
  }

  test("first") {
    val df = range(4) select (when('id % 2 === 0, lit(null)) otherwise 'id as 'x)

    checkDataFrame(
      df agg first('x, ignoresNull = true),
      Row(1)
    )

    checkDataFrame(
      df agg first('x, ignoresNull = false),
      Row(null)
    )
  }

  test("last") {
    val df = range(4) select (when('id % 2 =/= 0, lit(null)) otherwise 'id as 'x)

    checkDataFrame(
      df agg last('x, ignoresNull = true),
      Row(2)
    )

    checkDataFrame(
      df agg last('x, ignoresNull = false),
      Row(null)
    )
  }

  test("rand") {
    withTable('t) {
      range(10) asTable 't
      sql(
        """SELECT *
          |FROM (
          |  SELECT id AS key, CAST(RAND(42) * 100 AS INT) AS value
          |  FROM t
          |) s
          |WHERE value % 2 = 0
          |ORDER BY value DESC
          |""".stripMargin
      ).explanation(extended = true)
    }
  }
}

object LocalQueryCompilerSuite {
  case class Person(name: String, age: Int)
}
