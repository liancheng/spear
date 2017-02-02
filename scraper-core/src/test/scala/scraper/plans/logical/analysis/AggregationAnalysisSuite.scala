package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.IllegalAggregationException
import scraper.expressions._
import scraper.expressions.aggregates.AggregateFunction
import scraper.expressions.functions._
import scraper.expressions.windows.Window
import scraper.plans.logical.LocalRelation
import scraper.plans.logical.analysis.AggregationAnalysis.collectAggregateFunctions
import scraper.types.IntType

class AggregationAnalysisSuite extends AnalyzerTest { self =>
  test("global aggregate") {
    checkAnalyzedPlan(
      relation select 'count('a),
      relation resolvedAgg `@A: count(a)` select (`@A: count(a)`.attr as "count(a)")
    )
  }

  test("global aggregate in SQL") {
    val `@A: count(t.a)` = AggregationAlias(count(a of 't))

    checkAnalyzedPlan(
      "SELECT count(a) FROM t",

      relation
        subquery 't
        resolvedAgg `@A: count(t.a)`
        select (`@A: count(t.a)`.attr as "count(a)")
    )
  }

  test("aggregate with HAVING clause referencing projected attribute") {
    checkAnalyzedPlan(
      relation
        groupBy 'a
        agg ('count('b) as 'c)
        filter 'c > 1L,

      relation
        resolvedGroupBy `@G: a`
        agg `@A: count(b)`
        filter `@A: count(b)`.attr > 1L
        select (`@A: count(b)`.attr as 'c)
    )
  }

  test("aggregate with ORDER BY clause referencing projected attribute") {
    checkAnalyzedPlan(
      relation
        groupBy 'a
        agg ('count('b) as 'c)
        orderBy 'c.desc,

      relation
        resolvedGroupBy `@G: a`
        agg `@A: count(b)`
        orderBy `@A: count(b)`.attr.desc
        select (`@A: count(b)`.attr as 'c)
    )
  }

  test("aggregate with both HAVING and ORDER BY clauses") {
    checkAnalyzedPlan(
      relation groupBy 'a agg 'a filter 'a > 1 orderBy 'count('b).asc,

      relation
        resolvedGroupBy `@G: a`
        agg `@A: count(b)`
        filter `@G: a`.attr > 1
        orderBy `@A: count(b)`.attr.asc
        select (`@G: a`.attr as 'a)
    )
  }

  test("aggregate with multiple ORDER BY clauses") {
    checkAnalyzedPlan(
      relation
        groupBy 'a
        agg 'count('b)
        orderBy 'a.asc
        orderBy 'count('b).asc,

      relation
        resolvedGroupBy `@G: a`
        agg `@A: count(b)`
        // Only the last sort order should be preserved
        orderBy `@A: count(b)`.attr.asc
        select (`@A: count(b)`.attr as "count(b)")
    )
  }

  test("aggregate with multiple HAVING conditions") {
    checkAnalyzedPlan(
      relation
        groupBy 'a
        agg 'count('b)
        filter 'a > 1
        filter 'count('b) < 3L,

      relation
        resolvedGroupBy `@G: a`
        agg `@A: count(b)`
        // All `HAVING` conditions should be preserved
        filter `@G: a`.attr > 1 && `@A: count(b)`.attr < 3L
        select (`@A: count(b)`.attr as "count(b)")
    )
  }

  test("aggregate with multiple alternating HAVING and ORDER BY clauses") {
    checkAnalyzedPlan(
      relation
        groupBy 'a
        agg 'a
        filter 'a > 1
        orderBy 'a.asc
        filter 'count('b) < 10L
        orderBy 'count('b).asc,

      relation
        resolvedGroupBy `@G: a`
        agg `@A: count(b)`
        filter `@G: a`.attr > 1 && (`@A: count(b)`.attr < 10L)
        orderBy `@A: count(b)`.attr.asc
        select (`@G: a`.attr as 'a)
    )
  }

  test("aggregate with count(*)") {
    checkAnalyzedPlan(
      relation groupBy 'a agg 'count(*),

      relation
        resolvedGroupBy `@G: a`
        agg `@A: count(1)`
        select (`@A: count(1)`.attr as i"count(1)")
    )
  }

  test("illegal SELECT field") {
    val patterns = Seq(
      "Attribute a",
      "SELECT field (((a + 1) + a) + 1)",
      "[(a + 1)]"
    )

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(relation groupBy 'a + 1 agg a + 1 + a + 1)
    }
  }

  test("illegal HAVING condition") {
    val patterns = Seq(
      "Attribute b",
      "HAVING condition (b > 0)",
      "[a]"
    )

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(relation groupBy 'a agg 'count('a) filter 'b > 0)
    }
  }

  test("illegal ORDER BY expression") {
    val patterns = Seq(
      "Attribute b",
      "ORDER BY expression b ASC NULLS LAST",
      "[a]"
    )

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(relation groupBy 'a agg 'count('a) orderBy 'b)
    }
  }

  test("illegal nested aggregate function") {
    val patterns = Seq(
      "Aggregate function can't be nested within another aggregate function",
      "max(count(a))"
    )

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(relation agg 'max('count('a)))
    }
  }

  test("distinct") {
    checkAnalyzedPlan(
      relation.distinct,
      relation
        resolvedGroupBy (`@G: a`, `@G: b`)
        agg Nil
        select (`@G: a`.attr as 'a, `@G: b`.attr as 'b)
    )
  }

  test("collect aggregate functions") {
    def check(expected: AggregateFunction*)(actual: => Seq[AggregateFunction]): Unit = {
      assertResult(expected.toSet) {
        actual.toSet
      }
    }

    val a = 'a of IntType

    check(max(a)) {
      collectAggregateFunctions(max(a))
    }

    check() {
      collectAggregateFunctions(max(a) over ())
    }

    check(avg(a)) {
      collectAggregateFunctions(max(avg(a)) over ())
    }

    check(sum(a)) {
      collectAggregateFunctions(max(a) over (Window partitionBy sum(a)))
    }

    check(min(a)) {
      collectAggregateFunctions(max(a) over (Window orderBy min(a)))
    }

    check(sum(a), min(a)) {
      collectAggregateFunctions(max(a) over (Window partitionBy sum(a) orderBy min(a)))
    }

    check(avg(a), sum(a)) {
      collectAggregateFunctions(max(avg(a)) over (Window partitionBy sum(a)))
    }

    check(avg(a), min(a)) {
      collectAggregateFunctions(max(avg(a)) over (Window orderBy min(a)))
    }

    check(avg(a), sum(a), min(a)) {
      collectAggregateFunctions(max(avg(a)) over (Window partitionBy sum(a) orderBy min(a)))
    }

    check(avg(a), count(a), sum(a), min(a)) {
      collectAggregateFunctions(
        max(avg(a) + count(a)) over (Window partitionBy sum(a) orderBy min(a))
      )
    }
  }

  override protected def beforeAll(): Unit = catalog.registerRelation('t, relation)

  override protected def afterAll(): Unit = catalog.removeRelation('t)

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val `@A: count(a)` = AggregationAlias(count(self.a))

  private val `@A: count(b)` = AggregationAlias(count(self.b))

  private val `@A: count(1)` = AggregationAlias(count(1))

  private val `@G: a` = GroupingAlias(self.a)

  private val `@G: b` = GroupingAlias(self.b)

  private val relation = LocalRelation.empty(a, b)
}
