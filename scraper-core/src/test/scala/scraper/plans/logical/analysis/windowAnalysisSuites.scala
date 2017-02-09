package scraper.plans.logical.analysis

import scraper.exceptions.IllegalAggregationException
import scraper.expressions._
import scraper.expressions.functions._
import scraper.expressions.windows._
import scraper.plans.logical.{let, table, LocalRelation, LogicalPlan}

abstract class WindowAnalysisTest extends AnalyzerTest { self =>
  override protected def afterAll(): Unit = catalog.removeRelation('t)

  protected val relation: LogicalPlan = {
    catalog.registerRelation('t, LocalRelation.empty('a.int.!, 'b.string.?))
    catalog.lookupRelation('t)
  }

  protected val Seq(a: AttributeRef, b: AttributeRef) = relation.output

  protected val f0: WindowFrame = WindowFrame(RowsFrame, UnboundedPreceding, CurrentRow)

  protected val f1: WindowFrame = WindowFrame(RangeFrame, Preceding(1), Following(1))

  // -------------------
  // Aggregation aliases
  // -------------------

  protected val `@A: max(a)` = AggregationAlias(max(a))

  protected val `@A: count(b)` = AggregationAlias(count(b))
}

class WindowAnalysisWithoutGroupBySuite extends WindowAnalysisTest { self =>
  test("single window function") {
    checkSQLAnalysis(
      """SELECT max(a) OVER (
        |  PARTITION BY a
        |  ORDER BY b
        |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |) AS win_max
        |FROM t
        |""".stripMargin,

      table('t) select ('max('a) over `?w0?` as 'win_max),

      relation
        window `@W: max(a) over w0`
        select (`@W: max(a) over w0`.attr as 'win_max)
    )
  }

  test("single window function with non-window expressions") {
    checkSQLAnalysis(
      """SELECT a + max(a) OVER (
        |  PARTITION BY a
        |  ORDER BY b
        |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |) as win_max
        |FROM t
        |""".stripMargin,

      table('t) select ('a + ('max('a) over `?w0?`) as 'win_max),

      relation
        window `@W: max(a) over w0`
        select (a + `@W: max(a) over w0`.attr as 'win_max)
    )
  }

  test("multiple window functions with the same window spec") {
    checkSQLAnalysis(
      """SELECT
        |  max(a) OVER (
        |    PARTITION BY a
        |    ORDER BY b
        |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |  ) AS win_max,
        |  count(b) OVER (
        |    PARTITION BY a
        |    ORDER BY b
        |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |  ) AS win_count
        |FROM t
        |""".stripMargin,

      table('t) select (
        'max('a) over `?w0?` as 'win_max,
        'count('b) over `?w0?` as 'win_count
      ),

      relation.window(
        `@W: max(a) over w0`,
        `@W: count(b) over w0`
      ).select(
        `@W: max(a) over w0`.attr as 'win_max,
        `@W: count(b) over w0`.attr as 'win_count
      )
    )
  }

  test("multiple window functions with the same window spec and non-window expressions") {
    checkSQLAnalysis(
      """SELECT
        |  a + max(a) OVER (
        |    PARTITION BY a
        |    ORDER BY b
        |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |  ) AS c0,
        |  count(b) OVER (
        |    PARTITION BY a
        |    ORDER BY b
        |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |  ) AS c1
        |FROM t
        |""".stripMargin,

      table('t) select (
        ('a + ('max('a) over `?w0?`)) as 'c0,
        'count('b) over `?w0?` as 'c1
      ),

      relation.window(
        `@W: max(a) over w0`,
        `@W: count(b) over w0`
      ).select(
        a + `@W: max(a) over w0`.attr as 'c0,
        `@W: count(b) over w0`.attr as 'c1
      )
    )
  }

  test("multiple window functions with different window specs") {
    checkSQLAnalysis(
      """SELECT
        |  max(a) OVER (
        |    PARTITION BY a
        |    ORDER BY b
        |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |  ) AS win_max,
        |  count(b) OVER (
        |    PARTITION BY b
        |    ORDER BY a
        |    RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING
        |  ) AS win_count
        |FROM t
        |""".stripMargin,

      table('t) select (
        'max('a) over `?w0?` as 'win_max,
        'count('b) over `?w1?` as 'win_count
      ),

      relation
        window `@W: max(a) over w0`
        window `@W: count(b) over w1`
        select (
          `@W: max(a) over w0`.attr as 'win_max,
          `@W: count(b) over w1`.attr as 'win_count
        )
    )
  }

  test("multiple window functions with different window specs and non-window expressions") {
    checkSQLAnalysis(
      """SELECT
        |  a + max(a) OVER (
        |    PARTITION BY a
        |    ORDER BY b
        |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |  ) AS win_max,
        |  count(b) OVER (
        |    PARTITION BY b
        |    ORDER BY a
        |    RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING
        |  ) AS win_count
        |FROM t
        |""".stripMargin,

      table('t) select (
        ('a + ('max('a) over `?w0?`)) as 'win_max,
        'count('b) over `?w1?` as 'win_count
      ),

      relation
        window `@W: max(a) over w0`
        window `@W: count(b) over w1`
        select (
          a + `@W: max(a) over w0`.attr as 'win_max,
          `@W: count(b) over w1`.attr as 'win_count
        )
    )
  }

  test("window function in ORDER BY clause") {
    checkSQLAnalysis(
      """SELECT *
        |FROM t
        |ORDER BY max(a) OVER (
        |  PARTITION BY a
        |  ORDER BY b
        |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |)
        |""".stripMargin,

      table('t) select * orderBy ('max('a) over `?w0?`),

      relation
        select (a, b)
        window `@W: max(a) over w0`
        orderBy `@W: max(a) over w0`.attr
        select (a, b)
    )
  }

  test("reference window function alias in ORDER BY clause") {
    val win_max = `@W: max(a) over w0`.attr as 'win_max

    checkSQLAnalysis(
      """SELECT max(a) OVER (
        |  PARTITION BY a
        |  ORDER BY b
        |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |) AS win_max
        |FROM t
        |ORDER BY win_max
        |""".stripMargin,

      table('t)
        .select('max('a) over `?w0?` as 'win_max)
        .orderBy('win_max),

      relation
        .window(`@W: max(a) over w0`)
        .select(win_max)
        .orderBy(win_max.attr)
    )
  }

  test("single window definition") {
    checkSQLAnalysis(
      """SELECT max(a) OVER w0 AS win_max
        |FROM t
        |WINDOW w0 AS (
        |  PARTITION BY a
        |  ORDER BY b
        |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |)
        |""".stripMargin,

      let('w0, `?w0?`) {
        table('t) select ('max('a) over 'w0 as 'win_max)
      },

      relation
        window `@W: max(a) over w0`
        select (`@W: max(a) over w0`.attr as 'win_max)
    )
  }

  test("multiple window definitions") {
    checkSQLAnalysis(
      """SELECT
        |  max(a) OVER w0 AS win_max,
        |  count(b) OVER w1 AS win_count
        |FROM t
        |WINDOW
        |  w0 AS (
        |    PARTITION BY a
        |    ORDER BY b
        |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |  ),
        |  w1 AS (
        |    PARTITION BY b
        |    ORDER BY a
        |    RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING
        |  )
        |""".stripMargin,

      let('w0, `?w0?`) {
        let('w1, `?w1?`) {
          table('t) select (
            'max('a) over 'w0 as 'win_max,
            'count('b) over 'w1 as 'win_count
          )
        }
      },

      relation
        window `@W: max(a) over w0`
        window `@W: count(b) over w1`
        select (
          `@W: max(a) over w0`.attr as 'win_max,
          `@W: count(b) over w1`.attr as 'win_count
        )
    )
  }

  test("reference to existing window definition") {
    checkSQLAnalysis(
      """SELECT
        |  max(a) OVER w0 AS win_max,
        |  count(b) OVER w1 AS win_count
        |FROM t
        |WINDOW
        |  w0 AS (
        |    PARTITION BY a
        |    ORDER BY b
        |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |  ),
        |  w1 AS (w0)
        |""".stripMargin,

      let('w0, `?w0?`) {
        let('w1, 'w0) {
          table('t) select (
            'max('a) over 'w0 as 'win_max,
            'count('b) over 'w1 as 'win_count
          )
        }
      },

      relation.window(
        `@W: max(a) over w0`,
        `@W: count(b) over w0`
      ).select(
        `@W: max(a) over w0`.attr as 'win_max,
        `@W: count(b) over w0`.attr as 'win_count
      )
    )
  }

  // -----------------------
  // Unresolved window specs
  // -----------------------

  private val `?w0?` = Window partitionBy 'a orderBy 'b between f0

  private val `?w1?` = Window partitionBy 'b orderBy 'a between f1

  // ---------------------
  // Resolved window specs
  // ---------------------

  private val w0 = Expression.tryResolve(relation.output)(`?w0?`)

  private val w1 = Expression.tryResolve(relation.output)(`?w1?`)

  // --------------
  // Window aliases
  // --------------

  private val `@W: max(a) over w0` = WindowAlias(max(a) over w0)

  private val `@W: count(b) over w0` = WindowAlias(count(b) over w0)

  private val `@W: count(b) over w1` = WindowAlias(count(b) over w1)
}

class WindowAnalysisWithGroupBySuite extends WindowAnalysisTest {
  test("single window function") {
    checkSQLAnalysis(
      """SELECT count(a + 1) OVER (
        |  PARTITION BY a + 1
        |  ORDER BY b
        |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |) AS count
        |FROM t
        |GROUP BY a + 1, b
        |""".stripMargin,

      table('t) groupBy ('a + 1, 'b) agg (
        'count('a + 1) over `?w0?` as 'count
      ),

      relation
        .resolvedGroupBy(`@G: a + 1`, `@G: b`)
        .agg(Nil)
        .window(`@W: count(a + 1) over w0`)
        .select(`@W: count(a + 1) over w0`.attr as 'count)
    )
  }

  test("single window function with non-window aggregate function") {
    checkSQLAnalysis(
      """SELECT
        |  count(b) OVER (
        |    PARTITION BY a + 1
        |    ORDER BY b
        |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |  ) AS win_count,
        |  count(b) AS agg_count
        |FROM t
        |GROUP BY a + 1, b
        |""".stripMargin,

      table('t) groupBy ('a + 1, 'b) agg (
        'count('b) over `?w0?` as 'win_count,
        'count('b) as 'agg_count
      ),

      relation
        resolvedGroupBy (`@G: a + 1`, `@G: b`)
        agg `@A: count(b)`
        window `@W: count(b) over w0`
        select (
          `@W: count(b) over w0`.attr as 'win_count,
          `@A: count(b)`.attr as 'agg_count
        )
    )
  }

  test("multiple window functions with the same window spec") {
    checkSQLAnalysis(
      """SELECT
        |  count(a + 1) OVER (
        |    PARTITION BY a + 1
        |    ORDER BY b
        |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |  ) AS win_count0,
        |  count(b) OVER (
        |    PARTITION BY a + 1
        |    ORDER BY b
        |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |  ) AS win_count1
        |FROM t
        |GROUP BY a + 1, b
        |""".stripMargin,

      table('t) groupBy ('a + 1, 'b) agg (
        'count('a + 1) over `?w0?` as 'win_count0,
        'count('b) over `?w0?` as 'win_count1
      ),

      relation.resolvedGroupBy(`@G: a + 1`, `@G: b`).agg(Nil).window(
        `@W: count(a + 1) over w0`,
        `@W: count(b) over w0`
      ).select(
        `@W: count(a + 1) over w0`.attr as 'win_count0,
        `@W: count(b) over w0`.attr as 'win_count1
      )
    )
  }

  test("multiple window functions with multiple window spec") {
    checkSQLAnalysis(
      """SELECT
        |  count(a + 1) OVER (
        |    PARTITION BY a + 1
        |    ORDER BY b
        |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |  ) AS win_count0,
        |  count(b) OVER (
        |    PARTITION BY b
        |    ORDER BY a + 1
        |    RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING
        |  ) AS win_count1
        |FROM t
        |GROUP BY a + 1, b
        |""".stripMargin,

      table('t) groupBy ('a + 1, 'b) agg (
        'count('a + 1) over `?w0?` as 'win_count0,
        'count('b) over `?w1?` as 'win_count1
      ),

      relation
        resolvedGroupBy (`@G: a + 1`, `@G: b`)
        agg Nil
        window `@W: count(a + 1) over w0`
        window `@W: count(b) over w1`
        select (
          `@W: count(a + 1) over w0`.attr as 'win_count0,
          `@W: count(b) over w1`.attr as 'win_count1
        )
    )
  }

  test("aggregate function inside window spec") {
    checkSQLAnalysis(
      """SELECT count(b) OVER (
        |  PARTITION BY max(a)
        |) AS win_count
        |FROM t
        |GROUP BY a + 1, b
        |""".stripMargin,

      table('t) groupBy ('a + 1, 'b) agg (
        'count('b) over `?w2?` as 'win_count
      ),

      relation
        resolvedGroupBy (`@G: a + 1`, `@G: b`)
        agg `@A: max(a)`
        window `@W: count(b) over w2`
        select (`@W: count(b) over w2`.attr as 'win_count)
    )
  }

  test("non-window aggregate function inside window aggregate function") {
    val `@A: avg(a)` = AggregationAlias(avg(a))
    val `@W: max(avg(a)) over w0` = WindowAlias(max(`@A: avg(a)`.attr) over w0)

    checkSQLAnalysis(
      """SELECT max(avg(a)) OVER (
        |  PARTITION BY a + 1
        |  ORDER BY b
        |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |) AS win_max_of_avg
        |FROM t
        |GROUP BY a + 1, b
        |""".stripMargin,

      table('t) groupBy ('a + 1, 'b) agg (
        'max('avg('a)) over `?w0?` as 'win_max_of_avg
      ),

      relation
        resolvedGroupBy (`@G: a + 1`, `@G: b`)
        agg `@A: avg(a)`
        window `@W: max(avg(a)) over w0`
        select (`@W: max(avg(a)) over w0`.attr as 'win_max_of_avg)
    )
  }

  test("window function in ORDER BY clause") {
    checkSQLAnalysis(
      """SELECT a + 1 AS key
        |FROM t
        |GROUP BY a + 1, b
        |ORDER BY count(a + 1) OVER (
        |  PARTITION BY a + 1
        |  ORDER BY b
        |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |)
        |""".stripMargin,

      table('t)
        groupBy ('a + 1, 'b)
        agg ('a + 1 as 'key)
        orderBy ('count('a + 1) over `?w0?`),

      relation
        resolvedGroupBy (`@G: a + 1`, `@G: b`)
        agg Nil
        window `@W: count(a + 1) over w0`
        orderBy `@W: count(a + 1) over w0`.attr
        select (`@G: a + 1`.attr as 'key)
    )
  }

  test("reference window function alias in ORDER BY clause") {
    checkSQLAnalysis(
      """SELECT count(a + 1) OVER (
        |  PARTITION BY a + 1
        |  ORDER BY b
        |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |) AS win_count
        |FROM t
        |GROUP BY a + 1, b
        |ORDER BY win_count
        |""".stripMargin,

      table('t)
        groupBy ('a + 1, 'b)
        agg ('count('a + 1) over `?w0?` as 'win_count)
        orderBy 'win_count,

      relation
        resolvedGroupBy (`@G: a + 1`, `@G: b`)
        agg Nil
        window `@W: count(a + 1) over w0`
        orderBy `@W: count(a + 1) over w0`.attr
        select (`@W: count(a + 1) over w0`.attr as 'win_count)
    )
  }

  test("multiple window definitions") {
    checkSQLAnalysis(
      """SELECT
        |  count(a + 1) OVER w0 AS win_count0,
        |  count(b) OVER w1 AS win_count1
        |FROM t
        |GROUP BY a + 1, b
        |WINDOW
        |  w0 AS (
        |    PARTITION BY a + 1
        |    ORDER BY b
        |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |  ),
        |  w1 AS (
        |    PARTITION BY b
        |    ORDER BY a + 1
        |    RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING
        |  )
        |""".stripMargin,

      let('w0, `?w0?`) {
        let('w1, `?w1?`) {
          table('t) groupBy ('a + 1, 'b) agg (
            'count('a + 1) over 'w0 as 'win_count0,
            'count('b) over 'w1 as 'win_count1
          )
        }
      },

      relation
        resolvedGroupBy (`@G: a + 1`, `@G: b`)
        agg Nil
        window `@W: count(a + 1) over w0`
        window `@W: count(b) over w1`
        select (
          `@W: count(a + 1) over w0`.attr as 'win_count0,
          `@W: count(b) over w1`.attr as 'win_count1
        )
    )
  }

  test("reference to existing window definition") {
    checkSQLAnalysis(
      """SELECT
        |  count(a + 1) OVER w0 AS win_count0,
        |  count(b) OVER w1 AS win_count1
        |FROM t
        |GROUP BY a + 1, b
        |WINDOW
        |  w0 AS (
        |    PARTITION BY a + 1
        |    ORDER BY b
        |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |  ),
        |  w1 AS (w0)
        |""".stripMargin,

      let('w0, `?w0?`) {
        let('w1, 'w0) {
          table('t) groupBy ('a + 1, 'b) agg (
            'count('a + 1) over 'w0 as 'win_count0,
            'count('b) over 'w1 as 'win_count1
          )
        }
      },

      relation
        .resolvedGroupBy(`@G: a + 1`, `@G: b`)
        .agg(Nil)
        .window(
          `@W: count(a + 1) over w0`,
          `@W: count(b) over w0`
        )
        .select(
          `@W: count(a + 1) over w0`.attr as 'win_count0,
          `@W: count(b) over w0`.attr as 'win_count1
        )
    )
  }

  test("illegal window function in HAVING clause") {
    val patterns = Seq("Window functions are not allowed in HAVING clauses.")

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(
        relation
          groupBy 'a + 1
          agg 'a + 1
          filter ('count('a + 1) over `?w0?`)
      )
    }
  }

  test("illegal window function alias referenced in HAVING clause") {
    val patterns = Seq("Window functions are not allowed in HAVING clauses.")

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(
        relation
          groupBy 'a + 1
          agg ('count('a + 1) over `?w0?` as 'win_count)
          filter 'win_count > 1
      )
    }
  }

  test("illegal attribute reference in window function in SELECT clause") {
    val patterns = Seq(
      "Attribute t.a",
      s"window function ${count(a).over().sqlLike}",
      "[(t.a + 1)]"
    )

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(
        relation
          groupBy 'a + 1
          agg ('count('a) over () as 'win_count)
      )
    }
  }

  test("illegal attribute reference in window function in ORDER BY clause") {
    val patterns = Seq(
      "Attribute t.a",
      s"window function ${count(a).over().sqlLike}",
      "[(t.a + 1)]"
    )

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(
        relation
          groupBy 'a + 1
          agg Nil
          orderBy ('count('a) over ())
      )
    }
  }

  test("illegal attribute reference in window spec in SELECT clause") {
    val patterns = Seq(
      "Attribute t.b",
      s"window function ${count(a + 1).over(Window partitionBy b).sqlLike}",
      "[(t.a + 1)]"
    )

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(
        relation
          groupBy 'a + 1
          agg ('count('a + 1) over (Window partitionBy 'b) as 'win_count)
      )
    }
  }

  test("illegal attribute reference in window spec in ORDER BY clause") {
    val patterns = Seq(
      "Attribute t.a",
      s"window function ${count(a + 1).over(Window partitionBy a).sqlLike}",
      "[(t.a + 1)]"
    )

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(
        relation
          groupBy 'a + 1
          agg Nil
          orderBy ('count('a + 1) over (Window partitionBy 'a))
      )
    }
  }

  // ----------------
  // Grouping aliases
  // ----------------

  private val `@G: a + 1` = GroupingAlias(a + 1)

  private val `@G: b` = GroupingAlias(b)

  // -----------------------
  // Unresolved window specs
  // -----------------------

  private val `?w0?` = Window partitionBy 'a + 1 orderBy 'b between f0

  private val `?w1?` = Window partitionBy 'b orderBy 'a + 1 between f1

  private val `?w2?` = Window partitionBy 'max('a)

  // ---------------------
  // Resolved window specs
  // ---------------------

  private val w0 = Window partitionBy `@G: a + 1`.attr orderBy `@G: b`.attr between f0

  private val w1 = Window partitionBy `@G: b`.attr orderBy `@G: a + 1`.attr between f1

  private val w2 = Window partitionBy `@A: max(a)`.attr

  // --------------
  // Window aliases
  // --------------

  private val `@W: count(a + 1) over w0` = WindowAlias(count(`@G: a + 1`.attr) over w0)

  private val `@W: count(b) over w0` = WindowAlias(count(`@G: b`.attr) over w0)

  private val `@W: count(b) over w1` = WindowAlias(count(`@G: b`.attr) over w1)

  private val `@W: count(b) over w2` = WindowAlias(count(`@G: b`.attr) over w2)
}
