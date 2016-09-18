package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.IllegalAggregationException
import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.logical.LocalRelation

class AggregationAnalysisSuite extends AnalyzerTest { self =>
  test("global aggregate") {
    checkAnalyzedPlan(
      relation select 'count('a),
      relation resolvedAgg `@A: count(a)` select (`@A: count(a)`.attr as "count(a)")
    )
  }

  test("global aggregate in SQL") {
    val `count(t.a)` = AggregationAlias(count(a of 't))

    checkAnalyzedPlan(
      "SELECT count(a) FROM t",
      relation
        subquery 't
        resolvedAgg `count(t.a)`
        select (`count(t.a)`.attr as "count(a)")
    )
  }

  test("aggregate with having clause referencing projected attribute") {
    checkAnalyzedPlan(
      relation groupBy 'a agg ('count('b) as 'c) having 'c > 1L,
      relation
        resolvedGroupBy `@G: a`
        agg `@A: count(b)`
        filter `@A: count(b)`.attr > 1L
        select (`@A: count(b)`.attr as 'c)
    )
  }

  test("aggregate with order by clause referencing projected attribute") {
    checkAnalyzedPlan(
      relation groupBy 'a agg ('count('b) as 'c) orderBy 'c.desc,
      relation
        resolvedGroupBy `@G: a`
        agg `@A: count(b)`
        orderBy `@A: count(b)`.attr.desc
        select (`@A: count(b)`.attr as 'c)
    )
  }

  test("aggregate with both having and order by clauses") {
    checkAnalyzedPlan(
      relation groupBy 'a agg 'a having 'a > 1 orderBy 'count('b).asc,
      relation
        resolvedGroupBy `@G: a`
        agg `@A: count(b)`
        having `@G: a`.attr > 1
        orderBy `@A: count(b)`.attr.asc
        select (`@G: a`.attr as 'a)
    )
  }

  test("aggregate with multiple order by clauses") {
    checkAnalyzedPlan(
      relation groupBy 'a agg 'count('b) orderBy 'a.asc orderBy 'count('b).asc,
      relation
        resolvedGroupBy `@G: a`
        agg `@A: count(b)`
        // Only the last sort order should be preserved
        orderBy `@A: count(b)`.attr.asc
        select (`@A: count(b)`.attr as "count(b)")
    )
  }

  test("aggregate with multiple having conditions") {
    checkAnalyzedPlan(
      relation groupBy 'a agg 'count('b) having 'a > 1 having 'count('b) < 3L,
      relation
        resolvedGroupBy `@G: a`
        agg `@A: count(b)`
        // All having conditions should be preserved
        having `@G: a`.attr > 1 && `@A: count(b)`.attr < 3L
        select (`@A: count(b)`.attr as "count(b)")
    )
  }

  test("aggregate with multiple alternating having and order by clauses") {
    checkAnalyzedPlan(
      relation
        groupBy 'a
        agg 'a
        having 'a > 1
        orderBy 'a.asc
        having 'count('b) < 10L
        orderBy 'count('b).asc,
      relation
        resolvedGroupBy `@G: a`
        agg `@A: count(b)`
        having `@G: a`.attr > 1 && (`@A: count(b)`.attr < 10L)
        orderBy `@A: count(b)`.attr.asc
        select (`@G: a`.attr as 'a)
    )
  }

  test("aggregate with count(*)") {
    checkAnalyzedPlan(
      relation
        groupBy 'a
        agg 'count(*),
      relation
        resolvedGroupBy `@G: a`
        agg `@A: count(1)`
        select (`@A: count(1)`.attr as i"count(1)")
    )
  }

  test("analyzed aggregate should not expose `GeneratedAttribute`s") {
    checkAnalyzedPlan(
      // The "a" in agg list will be replaced by a `GroupingAttribute` during resolution.  This
      // `GroupingAttribute` must be aliased to the original name in the final analyzed plan.
      relation groupBy 'a agg 'a,
      relation resolvedGroupBy `@G: a` agg Nil select (`@G: a`.attr as 'a)
    )
  }

  test("illegal aggregation") {
    intercept[IllegalAggregationException] {
      analyze(relation groupBy 'a agg 'b)
    }
  }

  test("illegal nested aggregate function") {
    intercept[IllegalAggregationException] {
      analyze(relation agg max('count('a)))
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

  override protected def beforeAll(): Unit = {
    catalog.registerRelation('t, relation)
  }

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val `@A: count(a)` = AggregationAlias(count(self.a))

  private val `@A: count(b)` = AggregationAlias(count(self.b))

  private val `@A: count(1)` = AggregationAlias(count(1))

  private val `@G: a` = GroupingAlias(self.a)

  private val `@G: b` = GroupingAlias(self.b)

  private val relation = LocalRelation.empty(a, b)
}
