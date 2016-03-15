package scraper.plans.logical

import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.expressions.{AggregationAlias, GroupingAlias}
import scraper.local.InMemoryCatalog
import scraper.plans.logical.dsl._
import scraper.{LoggingFunSuite, TestUtils}

class AnalyzerSuite extends LoggingFunSuite with TestUtils {
  private val analyze = new Analyzer(new InMemoryCatalog)

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val relation = LocalRelation.empty(a, b)

  test("resolve references") {
    checkPlan(
      analyze(relation select ('b, ('a + 1) as 's)),
      relation select (b, (a + 1) as 's)
    )
  }

  test("expand star") {
    checkPlan(
      analyze(relation select '*),
      relation select (a, b)
    )
  }

  test("expand star with qualifier") {
    checkPlan(
      analyze(relation as 'x join (relation as 'y) select $"x.*"),
      relation as 'x join (relation.newInstance() as 'y) select (a qualifiedBy 'x, b qualifiedBy 'x)
    )
  }

  test("self-join") {
    checkPlan(
      analyze(relation join relation),
      relation join relation.newInstance()
    )
  }

  test("duplicated aliases") {
    // Using analyzed plan here so that the expression ID of alias "a" is fixed.
    val plan = analyze(SingleRowRelation select (1 as 'a))

    checkPlan(
      analyze(plan union plan),
      // Note that the following two aliases have different expression IDs.
      SingleRowRelation select (1 as 'a) union (SingleRowRelation select (1 as 'a))
    )
  }

  test("global aggregate") {
    val Project(child, _) = analyze(relation select count('a))

    checkPlan(child, {
      val aggCountA = AggregationAlias(count(a))
      Aggregate(relation, Nil, Seq(aggCountA))
    })
  }

  test("aggregate with multiple order by clauses") {
    val Project(child, _) = analyze {
      relation groupBy 'a agg count('b) orderBy 'a.asc orderBy count('b).asc
    }

    checkPlan(child, {
      val groupA = GroupingAlias(a)
      val aggCountB = AggregationAlias(count(b))

      // Only the last sort order should be preserved
      val resolvedOrder = aggCountB.toAttribute.asc
      Aggregate(relation, Seq(groupA), Seq(aggCountB)) orderBy resolvedOrder
    })
  }

  test("aggregate with multiple having conditions") {
    val groupA = GroupingAlias(a)
    val aggCountB = AggregationAlias(count(b))

    val Project(child, _) = analyze {
      relation groupBy 'a agg count('b) having 'a > 1 having count('b) < 3L
    }

    checkPlan(child, {
      // All the having conditions should be preserved
      val resolvedCondition = groupA.toAttribute > 1 and aggCountB.toAttribute < 3L
      Aggregate(relation, Seq(groupA), Seq(aggCountB)) having resolvedCondition
    })
  }
}
