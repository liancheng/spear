package scraper.plans.logical

import scraper.expressions.GeneratedAttribute._
import scraper.expressions.GeneratedNamedExpression.{ForAggregation, ForGrouping}
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.local.InMemoryCatalog
import scraper.plans.logical.dsl._
import scraper.{LoggingFunSuite, TestUtils}

class AnalyzerSuite extends LoggingFunSuite with TestUtils {
  private val analyzer = new Analyzer(new InMemoryCatalog)

  private val (a, b) = ('a.int.!, 'b.string.?)

  test("resolve references") {
    val relation = LocalRelation.empty(a, b)

    checkPlan(
      analyzer resolve (relation select ('b, ('a + 1) as 's)),
      relation select (b, (a + 1) as 's)
    )
  }

  test("expand stars") {
    val relation = LocalRelation.empty(a, b)

    checkPlan(
      analyzer resolve (relation select '*),
      relation select (a, b)
    )
  }

  test("self-join") {
    val relation = LocalRelation.empty(a)

    checkPlan(
      analyzer resolve (relation join relation),
      relation join relation.newInstance()
    )
  }

  test("duplicated aliases") {
    // Using analyzed plan here so that the expression ID of alias "a" is fixed.
    val plan = analyzer resolve (SingleRowRelation select (1 as 'a))

    checkPlan(
      analyzer resolve (plan union plan),
      // Note that the following two aliases have different expression IDs.
      SingleRowRelation select (1 as 'a) union (SingleRowRelation select (1 as 'a))
    )
  }

  test("aggregate with multiple order by clauses") {
    val (a, b) = ('a.int.!, 'b.int.!)
    val analyzedPlan = analyzer {
      LocalRelation.empty(a, b) groupBy 'a agg count('b) orderBy 'a.asc orderBy count('b).asc
    }

    // There should be only a single Sort operator with a single sort order
    val Seq(Sort(child, Seq(sortOrder))) = analyzedPlan.collect { case sort: Sort => sort }

    // Expands all generated attributes referencing to grouping keys or aggregate functions
    val expandedSortOrder = sortOrder.expand(child, ForGrouping, ForAggregation)

    // Only the last sort order should be preserved
    assert(expandedSortOrder == count(b).asc)
  }
}
