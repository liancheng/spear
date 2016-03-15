package scraper.plans.logical

import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.expressions.{AggregationAlias, GroupingAlias}
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
    val relation = LocalRelation.empty(a, b)

    val Project(child, _) = analyzer {
      relation groupBy 'a agg count('b) orderBy 'a.asc orderBy count('b).asc
    }

    checkPlan(child, {
      val groupA = GroupingAlias(a)
      val aggCountB = AggregationAlias(count(b))
      // Only the last sort order should be preserved
      Aggregate(relation, Seq(groupA), Seq(aggCountB)) orderBy aggCountB.toAttribute.asc
    })
  }

  test("aggregate with multiple having conditions") {
    val relation = LocalRelation.empty(a, b)

    val Project(child, _) = analyzer {
      relation groupBy 'a agg count('b) having 'a > 1 having count('b) < 3L
    }

    checkPlan(child, {
      val groupA = GroupingAlias(a)
      val aggCountB = AggregationAlias(count(b))
      // All the having conditions should be preserved
      Aggregate(relation, Seq(groupA), Seq(aggCountB))
        .having(groupA.toAttribute > 1 and aggCountB.toAttribute < 3L)
    })
  }
}
