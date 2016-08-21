package scraper

import java.io.{ByteArrayOutputStream, PrintStream}

import org.scalatest.BeforeAndAfterAll

import scraper.exceptions.ResolutionFailureException
import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.logical.{LocalRelation, LogicalPlan}
import scraper.types.{IntType, StringType, StructType}

class DataFrameSuite extends LoggingFunSuite with TestUtils with BeforeAndAfterAll {
  private implicit val context = new TestContext

  import context._

  private val r1 = LocalRelation(Nil, 'a.int.! :: 'b.string.? :: Nil)

  private val r2 = LocalRelation(Nil, 'a.int.! :: 'b.string.? :: Nil)

  override protected def beforeAll(): Unit = {
    context.catalog.registerRelation('t, r1)
    context.catalog.registerRelation('s, r2)
  }

  test("column") {
    val df = range(10)
    assert(df('id) == df.queryExecution.analyzedPlan.output.head)

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
      r1 subquery 't select ('a, 'b as 'c)
    )
  }

  test("select arbitrary expression") {
    checkLogicalPlan(
      table('t) select ('a + 1, 'b cast IntType),
      r1 subquery 't select ('a + 1, 'b cast IntType)
    )
  }

  test("global aggregation using select") {
    checkLogicalPlan(
      table('t) select count('a),
      r1 subquery 't select count('a)
    )
  }

  test("filter") {
    checkLogicalPlan(
      table('t) where 'a > 3 filter 'b.notNull,
      r1 subquery 't filter 'a > 3 filter 'b.notNull
    )
  }

  test("limit") {
    checkLogicalPlan(
      table('t) limit 3,
      r1 subquery 't limit 3
    )
  }

  test("distinct") {
    checkLogicalPlan(
      table('t).distinct,
      (r1 subquery 't).distinct
    )
  }

  test("order by sort order") {
    checkLogicalPlan(
      table('t) orderBy 'a.asc.nullsFirst,
      r1 subquery 't orderBy 'a.asc.nullsFirst
    )
  }

  test("order by arbitrary expression") {
    checkLogicalPlan(
      table('t) orderBy 'a + 1,
      r1 subquery 't orderBy ('a + 1).asc
    )
  }

  test("subquery") {
    checkLogicalPlan(
      table('t) subquery 'x subquery 'y,
      r1 subquery 't subquery 'x subquery 'y
    )
  }

  test("join") {
    checkLogicalPlan(
      table('t) subquery 'x join (table('t) subquery 'y) on $"x.a" === $"y.a",
      r1 subquery 't subquery 'x join (r1 subquery 't subquery 'y) on ('a of 'x) === ('a of 'y)
    )
  }

  test("union") {
    checkLogicalPlan(
      table('t) union table('t),
      r1 subquery 't union (r1 subquery 't)
    )
  }

  test("intersect") {
    checkLogicalPlan(
      table('t) intersect table('s),
      r1 subquery 't intersect (r2 subquery 's)
    )
  }

  test("except") {
    checkLogicalPlan(
      table('t) except table('s),
      r1 subquery 't except (r2 subquery 's)
    )
  }

  test("global aggregation") {
    checkLogicalPlan(
      table('t) agg count('a),
      r1 subquery 't agg count('a)
    )
  }

  test("group by") {
    checkLogicalPlan(
      table('t) groupBy 'a agg count('b),
      r1 subquery 't groupBy 'a agg count('b)
    )
  }

  test("group by with having condition") {
    checkLogicalPlan(
      table('t) groupBy 'a agg count('b) having 'a > 3,
      r1 subquery 't groupBy 'a agg count('b) having 'a > 3
    )
  }

  test("asTable") {
    withTable('reverse) {
      val df = table('t) orderBy 'a.desc
      df asTable 'reverse

      checkLogicalPlan(
        table('reverse),
        df.queryExecution.analyzedPlan subquery 'reverse
      )
    }
  }

  test("explain") {
    val out = new PrintStream(new ByteArrayOutputStream())
    // These only check that no exceptions are thrown during analysis, optimization, and planning.
    table('t).explain(extended = false, out = out)
    table('t).explain(extended = true, out = out)
  }

  test("show") {
    val out = new PrintStream(new ByteArrayOutputStream())
    // These only check that no exceptions are thrown during analysis, optimization, planning and
    // query execution.
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
