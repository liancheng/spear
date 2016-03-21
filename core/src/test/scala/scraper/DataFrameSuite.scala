package scraper

import java.io.{ByteArrayOutputStream, PrintStream}

import org.scalatest.BeforeAndAfterAll

import scraper.exceptions.ResolutionFailureException
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.plans.logical.dsl._
import scraper.plans.logical.{LocalRelation, LogicalPlan}
import scraper.types.{IntType, StringType, StructType}

class DataFrameSuite extends LoggingFunSuite with TestUtils with BeforeAndAfterAll {
  private implicit val context = new TestContext

  import context._

  private val r1 = LocalRelation(Nil, 'a.int.! :: 'b.string.? :: Nil)

  private val r2 = LocalRelation(Nil, 'a.int.! :: 'b.string.? :: Nil)

  override protected def beforeAll(): Unit = {
    context.catalog.registerRelation("t", r1)
    context.catalog.registerRelation("s", r2)
  }

  test("column") {
    val df = range(10)
    assert(df('id) === df.queryExecution.analyzedPlan.output.head)

    intercept[ResolutionFailureException] {
      df('bad)
    }
  }

  test("context") {
    assert(range(10).context eq context)

  }

  test("inferred local data") {
    checkTree(
      (lift(1 -> "a", 2 -> "b") rename ("x", "y")).schema,
      StructType(
        'x -> IntType.!,
        'y -> StringType.?
      )
    )
  }

  test("select named expressions") {
    checkLogicalPlan(
      table('t) select ('a, 'b as 'c),
      r1 as 't select ('a, 'b as 'c)
    )
  }

  test("select arbitrary expression") {
    checkLogicalPlan(
      table('t) select ('a + 1, 'b cast IntType),
      r1 as 't select ('a + 1, 'b cast IntType)
    )
  }

  test("global aggregation using select") {
    checkLogicalPlan(
      table('t) select count('a),
      r1 as 't select count('a)
    )
  }

  test("filter") {
    checkLogicalPlan(
      table('t) where 'a > 3 filter 'b.notNull,
      r1 as 't filter 'a > 3 filter 'b.notNull
    )
  }

  test("limit") {
    checkLogicalPlan(
      table('t) limit 3,
      r1 as 't limit 3
    )
  }

  test("distinct") {
    checkLogicalPlan(
      table('t).distinct,
      (r1 as 't).distinct
    )
  }

  test("order by sort order") {
    checkLogicalPlan(
      table('t) orderBy 'a.asc.nullsFirst,
      r1 as 't orderBy 'a.asc.nullsFirst
    )
  }

  test("order by arbitrary expression") {
    checkLogicalPlan(
      table('t) orderBy 'a + 1,
      r1 as 't orderBy ('a + 1).asc
    )
  }

  test("subquery") {
    checkLogicalPlan(
      table('t) subquery 'x as 'y,
      r1 as 't as 'x as 'y
    )
  }

  test("join") {
    checkLogicalPlan(
      table('t) subquery 'x join (table('t) subquery 'y) on $"x.a" =:= $"y.a",
      r1 as 't as 'x join (r1 as 't as 'y) on ('a of 'x) =:= ('a of 'y)
    )
  }

  test("union") {
    checkLogicalPlan(
      table('t) union table('t),
      r1 as 't union (r1 as 't)
    )
  }

  test("intersect") {
    checkLogicalPlan(
      table('t) intersect table('s),
      r1 as 't intersect (r2 as 's)
    )
  }

  test("except") {
    checkLogicalPlan(
      table('t) except table('s),
      r1 as 't except (r2 as 's)
    )
  }

  test("global aggregation") {
    checkLogicalPlan(
      table('t) agg count('a),
      r1 as 't agg count('a)
    )
  }

  test("group by") {
    checkLogicalPlan(
      table('t) groupBy 'a agg count('b),
      r1 as 't groupBy 'a agg count('b)
    )
  }

  test("group by with having condition") {
    checkLogicalPlan(
      table('t) groupBy 'a agg count('b) having 'a > 3,
      r1 as 't groupBy 'a agg count('b) having 'a > 3
    )
  }

  test("asTable") {
    withTable("reverse") {
      val df = table('t) orderBy 'a.desc
      df asTable 'reverse

      checkLogicalPlan(
        table('reverse),
        df.queryExecution.analyzedPlan as 'reverse
      )
    }
  }

  test("explain") {
    val out = new PrintStream(new ByteArrayOutputStream())
    table('t).explain(extended = false, out = out)
    table('t).explain(extended = true, out = out)
  }

  test("show") {
    val out = new PrintStream(new ByteArrayOutputStream())
    table('t).show(out = out)
  }

  test("showSchema") {
    val out = new ByteArrayOutputStream()
    val printStream = new PrintStream(out)
    val df = table('t)

    df.showSchema(out = printStream)
    assertSideBySide(
      out.toString,
      table('t).schema.prettyTree + "\n"
    )
  }

  private def checkLogicalPlan(df: DataFrame, expected: LogicalPlan): Unit = {
    checkPlan(df.queryExecution.logicalPlan, expected)
    // Triggers analysis phase
    df.queryExecution.analyzedPlan
  }
}
