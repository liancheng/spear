package scraper.plans.logical.analysis

import scraper.exceptions.IllegalAggregationException
import scraper.expressions._
import scraper.expressions.functions._
import scraper.expressions.windows._
import scraper.plans.logical.{let, table}

class WindowAnalysisWithGroupBySuite extends WindowAnalysisTest {
  test("single window function") {
    val w0_? = Window partitionBy 'a + 1 orderBy 'b rowsBetween (UnboundedPreceding, 0)

    val `@G: a + 1` = GroupingAlias(a + 1)
    val `@G: b` = GroupingAlias(b)

    val w0 = w0_? partitionBy `@G: a + 1`.attr orderBy `@G: b`.attr

    val `@W: count(a + 1) over w0` = WindowAlias(count(`@G: a + 1`.attr) over w0)

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
        'count('a + 1) over w0_? as 'count
      ),

      relation
        .aggregate(`@G: a + 1` :: `@G: b` :: Nil, Nil)
        .window(`@W: count(a + 1) over w0`)
        .select(`@W: count(a + 1) over w0`.attr as 'count)
    )
  }

  test("single window function with non-window aggregate function") {
    val w0_? = Window partitionBy 'a + 1 orderBy 'b rowsBetween (UnboundedPreceding, 0)

    val `@G: a + 1` = GroupingAlias(a + 1)
    val `@G: b` = GroupingAlias(b)

    val w0 = w0_? partitionBy `@G: a + 1`.attr orderBy `@G: b`.attr

    val `@W: count(b) over w0` = WindowAlias(count(`@G: b`.attr) over w0)
    val `@A: count(b)` = AggregationAlias(count(`@G: b`.attr))

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
        'count('b) over w0_? as 'win_count,
        'count('b) as 'agg_count
      ),

      relation
        aggregate (`@G: a + 1` :: `@G: b` :: Nil, `@A: count(b)` :: Nil)
        window `@W: count(b) over w0`
        select (
          `@W: count(b) over w0`.attr as 'win_count,
          `@A: count(b)`.attr as 'agg_count
        )
    )
  }

  test("multiple window functions with the same window spec") {
    val w0_? = Window partitionBy 'a + 1 orderBy 'b rowsBetween (UnboundedPreceding, 0)

    val `@G: a + 1` = GroupingAlias(a + 1)
    val `@G: b` = GroupingAlias(b)

    val w0 = w0_? partitionBy `@G: a + 1`.attr orderBy `@G: b`.attr

    val `@W: count(a + 1) over w0` = WindowAlias(count(`@G: a + 1`.attr) over w0)
    val `@W: count(b) over w0` = WindowAlias(count(`@G: b`.attr) over w0)

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
        'count('a + 1) over w0_? as 'win_count0,
        'count('b) over w0_? as 'win_count1
      ),

      relation
        .aggregate(`@G: a + 1` :: `@G: b` :: Nil, Nil)
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

  test("multiple window functions with multiple window spec") {
    val w0_? = Window partitionBy 'a + 1 orderBy 'b rowsBetween (UnboundedPreceding, 0)
    val w1_? = Window partitionBy 'b orderBy 'a + 1 rangeBetween (-1, 1)

    val `@G: a + 1` = GroupingAlias(a + 1)
    val `@G: b` = GroupingAlias(b)

    val w0 = w0_? partitionBy `@G: a + 1`.attr orderBy `@G: b`.attr
    val w1 = w1_? partitionBy `@G: b`.attr orderBy `@G: a + 1`.attr

    val `@W: count(a + 1) over w0` = WindowAlias(count(`@G: a + 1`.attr) over w0)
    val `@W: count(b) over w1` = WindowAlias(count(`@G: b`.attr) over w1)

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
        'count('a + 1) over w0_? as 'win_count0,
        'count('b) over w1_? as 'win_count1
      ),

      relation
        aggregate (`@G: a + 1` :: `@G: b` :: Nil, Nil)
        window `@W: count(a + 1) over w0`
        window `@W: count(b) over w1`
        select (
          `@W: count(a + 1) over w0`.attr as 'win_count0,
          `@W: count(b) over w1`.attr as 'win_count1
        )
    )
  }

  test("aggregate function inside window spec") {
    val w2_? = Window partitionBy 'max('a)

    val `@G: a + 1` = GroupingAlias(a + 1)
    val `@A: max(a)` = AggregationAlias(max(a))

    val w2 = Window partitionBy `@A: max(a)`.attr

    val `@G: b` = GroupingAlias(b)
    val `@W: count(b) over w2` = WindowAlias(count(`@G: b`.attr) over w2)

    checkSQLAnalysis(
      """SELECT count(b) OVER (
        |  PARTITION BY max(a)
        |) AS win_count
        |FROM t
        |GROUP BY a + 1, b
        |""".stripMargin,

      table('t) groupBy ('a + 1, 'b) agg (
        'count('b) over w2_? as 'win_count
      ),

      relation
        aggregate (`@G: a + 1` :: `@G: b` :: Nil, `@A: max(a)` :: Nil)
        window `@W: count(b) over w2`
        select (`@W: count(b) over w2`.attr as 'win_count)
    )
  }

  test("non-window aggregate function inside window aggregate function") {
    val w0_? = Window partitionBy 'a + 1 orderBy 'b rowsBetween (UnboundedPreceding, 0)

    val `@G: a + 1` = GroupingAlias(a + 1)
    val `@G: b` = GroupingAlias(b)

    val w0 = w0_? partitionBy `@G: a + 1`.attr orderBy `@G: b`.attr

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
        'max('avg('a)) over w0_? as 'win_max_of_avg
      ),

      relation
        aggregate (`@G: a + 1` :: `@G: b` :: Nil, `@A: avg(a)` :: Nil)
        window `@W: max(avg(a)) over w0`
        select (`@W: max(avg(a)) over w0`.attr as 'win_max_of_avg)
    )
  }

  test("window function in ORDER BY clause") {
    val w0_? = Window partitionBy 'a + 1 orderBy 'b rowsBetween (UnboundedPreceding, 0)

    val `@G: a + 1` = GroupingAlias(a + 1)
    val `@G: b` = GroupingAlias(b)

    val w0 = w0_? partitionBy `@G: a + 1`.attr orderBy `@G: b`.attr

    val `@W: count(a + 1) over w0` = WindowAlias(count(`@G: a + 1`.attr) over w0)

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
        sort ('count('a + 1) over w0_?),

      relation
        aggregate (`@G: a + 1` :: `@G: b` :: Nil, Nil)
        window `@W: count(a + 1) over w0`
        sort `@W: count(a + 1) over w0`.attr
        select (`@G: a + 1`.attr as 'key)
    )
  }

  test("reference window function alias in ORDER BY clause") {
    val w0_? = Window partitionBy 'a + 1 orderBy 'b rowsBetween (UnboundedPreceding, 0)

    val `@G: a + 1` = GroupingAlias(a + 1)
    val `@G: b` = GroupingAlias(b)

    val w0 = w0_? partitionBy `@G: a + 1`.attr orderBy `@G: b`.attr

    val `@W: count(a + 1) over w0` = WindowAlias(count(`@G: a + 1`.attr) over w0)

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
        agg ('count('a + 1) over w0_? as 'win_count)
        sort 'win_count,

      relation
        aggregate (`@G: a + 1` :: `@G: b` :: Nil, Nil)
        window `@W: count(a + 1) over w0`
        sort `@W: count(a + 1) over w0`.attr
        select (`@W: count(a + 1) over w0`.attr as 'win_count)
    )
  }

  test("multiple window definitions") {
    val w0_? = Window partitionBy 'a + 1 orderBy 'b rowsBetween (UnboundedPreceding, 0)
    val w1_? = Window partitionBy 'b orderBy 'a + 1 rangeBetween (-1, 1)

    val `@G: a + 1` = GroupingAlias(a + 1)
    val `@G: b` = GroupingAlias(b)

    val w0 = w0_? partitionBy `@G: a + 1`.attr orderBy `@G: b`.attr
    val w1 = w1_? partitionBy `@G: b`.attr orderBy `@G: a + 1`.attr

    val `@W: count(a + 1) over w0` = WindowAlias(count(`@G: a + 1`.attr) over w0)
    val `@W: count(b) over w1` = WindowAlias(count(`@G: b`.attr) over w1)

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

      let('w0, w0_?) {
        let('w1, w1_?) {
          table('t) groupBy ('a + 1, 'b) agg (
            'count('a + 1) over 'w0 as 'win_count0,
            'count('b) over 'w1 as 'win_count1
          )
        }
      },

      relation
        aggregate (`@G: a + 1` :: `@G: b` :: Nil, Nil)
        window `@W: count(a + 1) over w0`
        window `@W: count(b) over w1`
        select (
          `@W: count(a + 1) over w0`.attr as 'win_count0,
          `@W: count(b) over w1`.attr as 'win_count1
        )
    )
  }

  test("reference to existing window definition") {
    val w0_? = Window partitionBy 'a + 1 orderBy 'b rowsBetween (UnboundedPreceding, 0)

    val `@G: a + 1` = GroupingAlias(a + 1)
    val `@G: b` = GroupingAlias(b)

    val w0 = w0_? partitionBy `@G: a + 1`.attr orderBy `@G: b`.attr

    val `@W: count(a + 1) over w0` = WindowAlias(count(`@G: a + 1`.attr) over w0)
    val `@W: count(b) over w0` = WindowAlias(count(`@G: b`.attr) over w0)

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

      let('w0, w0_?) {
        let('w1, 'w0) {
          table('t) groupBy ('a + 1, 'b) agg (
            'count('a + 1) over 'w0 as 'win_count0,
            'count('b) over 'w1 as 'win_count1
          )
        }
      },

      relation
        .aggregate(`@G: a + 1` :: `@G: b` :: Nil, Nil)
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

  test("illegal window function in grouping key") {
    val patterns = Seq("Window functions are not allowed in grouping key")

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(
        relation
          groupBy 'count('a).over()
          agg 'count(*)
      )
    }
  }

  test("illegal window function in HAVING clause") {
    val patterns = Seq("Window functions are not allowed in HAVING condition")

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(
        relation
          groupBy 'a + 1
          agg 'a + 1
          filter ('count('a + 1) over (
            Window partitionBy 'a + 1 orderBy 'b between WindowFrame(
              RowsFrame, UnboundedPreceding, CurrentRow
            )
          ))
      )
    }
  }

  test("illegal window function alias referenced in HAVING clause") {
    val patterns = Seq("Window functions are not allowed in HAVING condition")
    val f0 = Window partitionBy 'a + 1 orderBy 'b rowsBetween (UnboundedPreceding, 0)

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(
        relation
          groupBy 'a + 1
          agg ('count('a + 1) over f0 as 'win_count)
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
          sort ('count('a) over ())
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
          sort ('count('a + 1) over (Window partitionBy 'a))
      )
    }
  }
}
