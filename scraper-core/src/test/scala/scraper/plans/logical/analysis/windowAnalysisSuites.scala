package scraper.plans.logical.analysis

import scraper.expressions._
import scraper.expressions.functions._
import scraper.expressions.windows._
import scraper.plans.logical.LocalRelation

abstract class WindowAnalysisTest extends AnalyzerTest { self =>
  protected val (a, b) = ('a.int.!, 'b.string.?)

  protected val relation = LocalRelation.empty(a, b)

  protected val (f0, f1) = (
    WindowFrame(RowsFrame, UnboundedPreceding, CurrentRow),
    WindowFrame(RowsFrame, Preceding(1), Following(1))
  )

  protected val (w0, w1, w2, w3, w4) = (
    Window partitionBy 'a orderBy 'b.desc between f0,
    Window partitionBy 'b orderBy 'a.asc between f1,
    Window partitionBy 'a % 10 orderBy 'b.desc between f0,
    Window partitionBy 'b orderBy ('a % 10).asc between f1,
    Window partitionBy 'avg('a)
  )

  protected val (resolvedW0, resolvedW1) = (
    Window partitionBy a orderBy b.desc between f0,
    Window partitionBy b orderBy a.asc between f1
  )

  protected val `@G: a % 10` = GroupingAlias(a % 10)

  protected val `@G: b` = GroupingAlias(b)

  protected val `@A: avg(a)` = AggregationAlias(avg(a))

  protected val `@A: max(b)` = AggregationAlias(max(b))

  protected val `@A: count(b)` = AggregationAlias(count(b))

  protected val `@W: sum(a) over w0` = WindowAlias(sum(a) over resolvedW0)

  protected val `@W: max(b) over w0` = WindowAlias(max(b) over resolvedW0)

  protected val `@W: max(b) over w1` = WindowAlias(max(b) over resolvedW1)

  protected val (resolvedW2, resolvedW3, resolvedW4) = (
    Window partitionBy `@G: a % 10`.attr orderBy `@G: b`.attr.desc between f0,
    Window partitionBy `@G: b`.attr orderBy `@G: a % 10`.attr.asc between f1,
    Window partitionBy `@A: avg(a)`.attr
  )

  protected val `@W: sum(a % 10) over w2` = WindowAlias(sum(`@G: a % 10`.attr) over resolvedW2)

  protected val `@W: max(b) over w2` = WindowAlias(max(`@G: b`.attr) over resolvedW2)

  protected val `@W: max(b) over w3` = WindowAlias(max(`@G: b`.attr) over resolvedW3)

  protected val `@W: max(b) over w4` = WindowAlias(max(`@G: b`.attr) over resolvedW4)
}

class WindowAnalysisWithoutGroupBySuite extends WindowAnalysisTest { self =>
  test("1 window function") {
    checkAnalyzedPlan(
      relation.select('sum('a) over w0 as 'sum),

      relation
        .window(`@W: sum(a) over w0`)
        .select(`@W: sum(a) over w0`.attr as 'sum)
    )
  }

  test("1 window function with non-window expressions") {
    checkAnalyzedPlan(
      relation.select('a + ('sum('a) over w0) as 'sum),

      relation
        .window(`@W: sum(a) over w0`)
        .select(a + `@W: sum(a) over w0`.attr as 'sum)
    )
  }

  test("2 window functions with the same window spec") {
    checkAnalyzedPlan(
      relation.select(
        'sum('a) over w0 as 'sum,
        'max('b) over w0 as 'max
      ),

      relation.window(
        `@W: sum(a) over w0`,
        `@W: max(b) over w0`
      ).select(
        `@W: sum(a) over w0`.attr as 'sum,
        `@W: max(b) over w0`.attr as 'max
      )
    )
  }

  test("2 window functions with 1 window spec and non-window expressions") {
    checkAnalyzedPlan(
      relation.select(
        ('a + ('sum('a) over w0)) as 'x,
        'concat('b, 'max('b) over w0) as 'y
      ),

      relation.window(
        `@W: sum(a) over w0`,
        `@W: max(b) over w0`
      ).select(
        (a + `@W: sum(a) over w0`.attr) as 'x,
        concat(b, `@W: max(b) over w0`.attr) as 'y
      )
    )
  }

  test("2 window functions with 2 window specs") {
    checkAnalyzedPlan(
      relation.select(
        'sum('a) over w0 as 'sum,
        'max('b) over w1 as 'max
      ),

      relation
        .window(`@W: sum(a) over w0`)
        .window(`@W: max(b) over w1`)
        .select(
          `@W: sum(a) over w0`.attr as 'sum,
          `@W: max(b) over w1`.attr as 'max
        )
    )
  }

  test("2 window functions with 2 window specs and non-window expressions") {
    checkAnalyzedPlan(
      relation.select(
        ('a + ('sum('a) over w0)) as 'x,
        'concat('b, 'max('b) over w1) as 'y
      ),

      relation
        .window(`@W: sum(a) over w0`)
        .window(`@W: max(b) over w1`)
        .select(
          (a + `@W: sum(a) over w0`.attr) as 'x,
          concat(b, `@W: max(b) over w1`.attr) as 'y
        )
    )
  }
}

class WindowAnalysisWithGroupBySuite extends WindowAnalysisTest {
  test("1 window function") {
    checkAnalyzedPlan(
      relation
        .groupBy('a % 10, b)
        .agg('sum('a % 10) over w2 as 'sum),

      relation
        .resolvedGroupBy(`@G: a % 10`, `@G: b`)
        .agg(Nil)
        .window(`@W: sum(a % 10) over w2`)
        .select(`@W: sum(a % 10) over w2`.attr as 'sum)
    )
  }

  test("1 window function with non-window aggregate function") {
    checkAnalyzedPlan(
      relation
        .groupBy('a % 10, 'b)
        .agg(
          'max('b) over w2 as 'win_max,
          'max('b) as 'agg_max
        ),

      relation
        .resolvedGroupBy(`@G: a % 10`, `@G: b`)
        .agg(`@A: max(b)`)
        .window(`@W: max(b) over w2`)
        .select(
          `@W: max(b) over w2`.attr as 'win_max,
          `@A: max(b)`.attr as 'agg_max
        )
    )
  }

  test("2 window functions with 1 window spec") {
    checkAnalyzedPlan(
      relation
        .groupBy('a % 10, 'b)
        .agg(
          'sum('a % 10) over w2 as 'sum,
          'max('b) over w2 as 'max
        ),

      relation
        .resolvedGroupBy(`@G: a % 10`, `@G: b`)
        .agg(Nil)
        .window(
          `@W: sum(a % 10) over w2`,
          `@W: max(b) over w2`
        )
        .select(
          `@W: sum(a % 10) over w2`.attr as 'sum,
          `@W: max(b) over w2`.attr as 'max
        )
    )
  }

  test("2 window functions with 2 window spec") {
    checkAnalyzedPlan(
      relation
        .groupBy('a % 10, 'b)
        .agg(
          'sum('a % 10) over w2 as 'sum,
          'max('b) over w3 as 'max
        ),

      relation
        .resolvedGroupBy(`@G: a % 10`, `@G: b`)
        .agg(Nil)
        .window(`@W: sum(a % 10) over w2`)
        .window(`@W: max(b) over w3`)
        .select(
          `@W: sum(a % 10) over w2`.attr as 'sum,
          `@W: max(b) over w3`.attr as 'max
        )
    )
  }

  test("aggregate function within window spec") {
    checkAnalyzedPlan(
      relation
        .groupBy('a % 10, 'b)
        .agg('max('b) over w4 as 'win_max),

      relation
        .resolvedGroupBy(`@G: a % 10`, `@G: b`)
        .agg(`@A: avg(a)`)
        .window(`@W: max(b) over w4`)
        .select(`@W: max(b) over w4`.attr as 'win_max)
    )
  }

  test("complex all-star query") {
    checkAnalyzedPlan(
      relation
        .groupBy('a % 10, 'b)
        .agg(
          // Grouping keys
          'a % 10 as 'key1,
          'b as 'key2,
          // Window functions with different window specs
          'sum('a % 10) over w2 as 'win_sum,
          'max('b) over w3 as 'win_max1,
          // Non-window aggregate in window spec (w4)
          'max('b) over w4 as 'win_max2,
          // Non-window aggregate function
          'avg('a) as 'agg_avg
        )
        // Grouping key in HAVING clause
        .filter('a % 10 > 3)
        // Aggregate function in ORDER BY clause
        .orderBy('count('b).desc),

      relation
        .resolvedGroupBy(`@G: a % 10`, `@G: b`)
        .agg(`@A: avg(a)`, `@A: count(b)`)
        .filter(`@G: a % 10`.attr > 3)
        .window(`@W: sum(a % 10) over w2`)
        .window(`@W: max(b) over w3`)
        .window(`@W: max(b) over w4`)
        .orderBy(`@A: count(b)`.attr.desc)
        .select(
          `@G: a % 10`.attr as 'key1,
          `@G: b`.attr as 'key2,
          `@W: sum(a % 10) over w2`.attr as 'win_sum,
          `@W: max(b) over w3`.attr as 'win_max1,
          `@W: max(b) over w4`.attr as 'win_max2,
          `@A: avg(a)`.attr as 'agg_avg
        )
    )
  }
}
