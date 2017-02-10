package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.IllegalAggregationException
import scraper.expressions._
import scraper.expressions.aggregates.AggregateFunction
import scraper.expressions.functions._
import scraper.expressions.windows.Window
import scraper.plans.logical.{table, LocalRelation}
import scraper.plans.logical.analysis.AggregationAnalysis.collectAggregateFunctions
import scraper.types.{IntType, LongType}

class AggregationAnalysisSuite extends AnalyzerTest { self =>
  test("global aggregate") {
    checkSQLAnalysis(
      "SELECT count(a) FROM t",

      table('t) select 'count('a),

      relation resolvedAgg `@A: count(a)` select (`@A: count(a)`.attr as "count(a)")
    )
  }

  test("aggregate with HAVING clause referencing projected attribute") {
    checkSQLAnalysis(
      "SELECT count(b) AS c FROM t GROUP BY a HAVING c > 1",

      table('t)
        groupBy 'a
        agg ('count('b) as 'c)
        filter 'c > 1,

      relation
        resolvedGroupBy `@G: a`
        agg `@A: count(b)`
        filter `@A: count(b)`.attr > (1 cast LongType)
        select (`@A: count(b)`.attr as 'c)
    )
  }

  test("aggregate with ORDER BY clause referencing projected attribute") {
    checkSQLAnalysis(
      "SELECT count(b) AS c FROM t GROUP BY a ORDER BY c DESC",

      table('t)
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
    checkSQLAnalysis(
      "SELECT a FROM t GROUP BY a HAVING a > 1 ORDER BY count(b) ASC",

      table('t) groupBy 'a agg 'a filter 'a > 1 orderBy 'count('b).asc,

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
      table('t)
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
      table('t)
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
      table('t)
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
    checkSQLAnalysis(
      "SELECT count(*) FROM t GROUP BY a",

      table('t) groupBy 'a agg 'count(*),

      relation
        resolvedGroupBy `@G: a`
        agg `@A: count(1)`
        select (`@A: count(1)`.attr as i"count(1)")
    )
  }

  test("illegal SELECT field") {
    val patterns = Seq(
      "Attribute t.a",
      "SELECT field (((t.a + 1) + t.a) + 1)",
      "[(t.a + 1)]"
    )

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(table('t) groupBy 'a + 1 agg 'a + 1 + 'a + 1)
    }
  }

  test("illegal HAVING condition") {
    val patterns = Seq(
      "Attribute t.b",
      "HAVING condition (t.b > 0)",
      "[t.a]"
    )

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(table('t) groupBy 'a agg 'count('a) filter 'b > 0)
    }
  }

  test("illegal ORDER BY expression") {
    val patterns = Seq(
      "Attribute t.b",
      "ORDER BY expression t.b ASC NULLS LAST",
      "[t.a]"
    )

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(table('t) groupBy 'a agg 'count('a) orderBy 'b)
    }
  }

  test("illegal nested aggregate function") {
    val patterns = Seq(
      "Aggregate function can't be nested within another aggregate function",
      "max(count(t.a))"
    )

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(table('t) agg 'max('count('a)))
    }
  }

  test("distinct") {
    checkSQLAnalysis(
      "SELECT DISTINCT * FROM t",

      table('t).select(*).distinct,

      relation
        select (a, b)
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

  override protected def afterAll(): Unit = catalog.removeRelation('t)

  private val relation = {
    catalog.registerRelation('t, LocalRelation.empty('a.int.!, 'b.string.?))
    catalog.lookupRelation('t)
  }

  private val Seq(a: AttributeRef, b: AttributeRef) = relation.output

  private val `@A: count(a)` = AggregationAlias(count(self.a))

  private val `@A: count(b)` = AggregationAlias(count(self.b))

  private val `@A: count(1)` = AggregationAlias(count(1))

  private val `@G: a` = GroupingAlias(self.a)

  private val `@G: b` = GroupingAlias(self.b)
}
