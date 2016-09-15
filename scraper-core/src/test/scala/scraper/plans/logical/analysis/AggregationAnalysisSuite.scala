package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.IllegalAggregationException
import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.logical.LocalRelation

class AggregationAnalysisSuite extends AnalyzerTest {
  test("global aggregate") {
    checkAnalyzedPlan(
      relation0 select count('a),
      relation0 resolvedAgg aggCountA select (aggCountA.attr as "count(a)")
    )
  }

  test("global aggregate in SQL") {
    val aggCountA = AggregationAlias(count(a of 't))

    checkAnalyzedPlan(
      "SELECT count(a) FROM t",
      relation0 subquery 't resolvedAgg aggCountA select (aggCountA.attr as "count(a)")
    )
  }

  test("aggregate with both having and order by clauses") {
    checkAnalyzedPlan(
      relation0 groupBy 'a agg 'a having 'a > 1 orderBy count('b).asc,
      relation0
        resolvedGroupBy groupA
        agg aggCountB
        having groupA.attr > 1
        orderBy aggCountB.attr.asc
        select (groupA.attr as 'a)
    )
  }

  test("aggregate with multiple order by clauses") {
    checkAnalyzedPlan(
      relation0 groupBy 'a agg count('b) orderBy 'a.asc orderBy count('b).asc,
      relation0
        resolvedGroupBy groupA
        agg aggCountB
        // Only the last sort order should be preserved
        orderBy aggCountB.attr.asc
        select (aggCountB.attr as "count(b)")
    )
  }

  test("aggregate with multiple having conditions") {
    checkAnalyzedPlan(
      relation0 groupBy 'a agg count('b) having 'a > 1 having count('b) < 3L,
      relation0
        resolvedGroupBy groupA
        agg aggCountB
        // All having conditions should be preserved
        having groupA.attr > 1 && aggCountB.attr < 3L
        select (aggCountB.attr as "count(b)")
    )
  }

  test("aggregate with multiple alternating having and order by clauses") {
    checkAnalyzedPlan(
      relation0
        groupBy 'a
        agg 'a
        having 'a > 1
        orderBy 'a.asc
        having count('b) < 10L
        orderBy count('b).asc,
      relation0
        resolvedGroupBy groupA
        agg aggCountB
        having groupA.attr > 1 && (aggCountB.attr < 10L)
        orderBy aggCountB.attr.asc
        select (groupA.attr as 'a)
    )
  }

  test("aggregate with count(*)") {
    checkAnalyzedPlan(
      relation0
        groupBy 'a
        agg count(),
      relation0
        resolvedGroupBy groupA
        agg aggCount1
        select (aggCount1.attr as i"count(1)")
    )
  }

  test("analyzed aggregate should not expose `GeneratedAttribute`s") {
    checkAnalyzedPlan(
      // The "a" in agg list will be replaced by a `GroupingAttribute` during resolution.  This
      // `GroupingAttribute` must be aliased to the original name in the final analyzed plan.
      relation0 groupBy 'a agg 'a,
      relation0 resolvedGroupBy groupA agg Nil select (groupA.attr as 'a)
    )
  }

  test("illegal aggregation") {
    intercept[IllegalAggregationException] {
      analyze(relation0 groupBy 'a agg 'b)
    }
  }

  test("illegal nested aggregate function") {
    intercept[IllegalAggregationException] {
      analyze(relation0 agg max(count('a)))
    }
  }

  test("distinct") {
    checkAnalyzedPlan(
      relation0.distinct,
      relation0
        resolvedGroupBy (groupA, groupB)
        agg Nil
        select (groupA.attr as 'a, groupB.attr as 'b)
    )
  }

  override protected def beforeAll(): Unit = {
    catalog.registerRelation('t, relation0)
  }

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val (groupA, groupB, aggCountA, aggCountB, aggCount1) = (
    GroupingAlias(a),
    GroupingAlias(b),
    AggregationAlias(count(a)),
    AggregationAlias(count(b)),
    AggregationAlias(count(1))
  )

  private val relation0 = LocalRelation.empty(a, b)
}
