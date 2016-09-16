package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.IllegalAggregationException
import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.logical.LocalRelation

class AggregationAnalysisSuite extends AnalyzerTest { self =>
  test("global aggregate") {
    checkAnalyzedPlan(
      relation select count('a),
      relation resolvedAgg agg.`count(a)` select (agg.`count(a)`.attr as "count(a)")
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
      relation groupBy 'a agg (count('b) as 'c) having 'c > 1L,
      relation
        resolvedGroupBy group.a
        agg agg.`count(b)`
        filter agg.`count(b)`.attr > 1L
        select (agg.`count(b)`.attr as 'c)
    )
  }

  test("aggregate with order by clause referencing projected attribute") {
    checkAnalyzedPlan(
      relation groupBy 'a agg (count('b) as 'c) orderBy 'c.desc,
      relation
        resolvedGroupBy group.a
        agg agg.`count(b)`
        orderBy agg.`count(b)`.attr.desc
        select (agg.`count(b)`.attr as 'c)
    )
  }

  test("aggregate with both having and order by clauses") {
    checkAnalyzedPlan(
      relation groupBy 'a agg 'a having 'a > 1 orderBy count('b).asc,
      relation
        resolvedGroupBy group.a
        agg agg.`count(b)`
        having group.a.attr > 1
        orderBy agg.`count(b)`.attr.asc
        select (group.a.attr as 'a)
    )
  }

  test("aggregate with multiple order by clauses") {
    checkAnalyzedPlan(
      relation groupBy 'a agg count('b) orderBy 'a.asc orderBy count('b).asc,
      relation
        resolvedGroupBy group.a
        agg agg.`count(b)`
        // Only the last sort order should be preserved
        orderBy agg.`count(b)`.attr.asc
        select (agg.`count(b)`.attr as "count(b)")
    )
  }

  test("aggregate with multiple having conditions") {
    checkAnalyzedPlan(
      relation groupBy 'a agg count('b) having 'a > 1 having count('b) < 3L,
      relation
        resolvedGroupBy group.a
        agg agg.`count(b)`
        // All having conditions should be preserved
        having group.a.attr > 1 && agg.`count(b)`.attr < 3L
        select (agg.`count(b)`.attr as "count(b)")
    )
  }

  test("aggregate with multiple alternating having and order by clauses") {
    checkAnalyzedPlan(
      relation
        groupBy 'a
        agg 'a
        having 'a > 1
        orderBy 'a.asc
        having count('b) < 10L
        orderBy count('b).asc,
      relation
        resolvedGroupBy group.a
        agg agg.`count(b)`
        having group.a.attr > 1 && (agg.`count(b)`.attr < 10L)
        orderBy agg.`count(b)`.attr.asc
        select (group.a.attr as 'a)
    )
  }

  test("aggregate with count(*)") {
    checkAnalyzedPlan(
      relation
        groupBy 'a
        agg count(),
      relation
        resolvedGroupBy group.a
        agg agg.`count(1)`
        select (agg.`count(1)`.attr as i"count(1)")
    )
  }

  test("analyzed aggregate should not expose `GeneratedAttribute`s") {
    checkAnalyzedPlan(
      // The "a" in agg list will be replaced by a `GroupingAttribute` during resolution.  This
      // `GroupingAttribute` must be aliased to the original name in the final analyzed plan.
      relation groupBy 'a agg 'a,
      relation resolvedGroupBy group.a agg Nil select (group.a.attr as 'a)
    )
  }

  test("illegal aggregation") {
    intercept[IllegalAggregationException] {
      analyze(relation groupBy 'a agg 'b)
    }
  }

  test("illegal nested aggregate function") {
    intercept[IllegalAggregationException] {
      analyze(relation agg max(count('a)))
    }
  }

  test("distinct") {
    checkAnalyzedPlan(
      relation.distinct,
      relation
        resolvedGroupBy (group.a, group.b)
        agg Nil
        select (group.a.attr as 'a, group.b.attr as 'b)
    )
  }

  override protected def beforeAll(): Unit = {
    catalog.registerRelation('t, relation)
  }

  private val (a, b) = ('a.int.!, 'b.string.?)

  private object agg {
    val `count(a)` = AggregationAlias(count(self.a))

    val `count(b)` = AggregationAlias(count(self.b))

    val `count(1)` = AggregationAlias(count(1))
  }

  private object group {
    val a = GroupingAlias(self.a)

    val b = GroupingAlias(self.b)
  }

  private val relation = LocalRelation.empty(a, b)
}
