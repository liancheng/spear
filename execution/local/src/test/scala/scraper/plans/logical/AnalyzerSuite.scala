package scraper.plans.logical

import scraper.expressions.NamedExpression.newExpressionID
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.local.InMemoryCatalog
import scraper.plans.logical.dsl._
import scraper.{LoggingFunSuite, TestUtils}

class AnalyzerSuite extends LoggingFunSuite with TestUtils {
  private val analyze = new Analyzer(new InMemoryCatalog)

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val (groupA, aggCountA, aggCountB) = (a.asGrouping, count(a).asAgg, count(b).asAgg)

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
    val alias = 1 as 'a
    val newAlias = alias withID newExpressionID()

    checkPlan(
      analyze(SingleRowRelation select alias union (SingleRowRelation select alias)),
      SingleRowRelation select alias union (SingleRowRelation select newAlias)
    )
  }

  test("global aggregate") {
    checkPlan(
      analyze(relation select count('a)),
      Aggregate(relation, Nil, Seq(aggCountA)) select (aggCountA.attr as "COUNT(a)")
    )
  }

  test("aggregate with both having and order by clauses") {
    checkPlan(
      analyze(relation groupBy 'a agg 'a having 'a > 1 orderBy count('b).asc),
      Aggregate(relation, Seq(groupA), Seq(aggCountB))
        .having(groupA.attr > 1)
        .orderBy(aggCountB.attr.asc)
        .select(groupA.attr as 'a)
    )
  }

  test("aggregate with multiple order by clauses") {
    checkPlan(
      analyze(relation groupBy 'a agg count('b) orderBy 'a.asc orderBy count('b).asc),
      Aggregate(relation, Seq(groupA), Seq(aggCountB))
        // Only the last sort order should be preserved
        .orderBy(aggCountB.attr.asc)
        .select(aggCountB.attr as "COUNT(b)")
    )
  }

  test("aggregate with multiple having conditions") {
    checkPlan(
      analyze(relation groupBy 'a agg count('b) having 'a > 1 having count('b) < 3L),
      Aggregate(relation, Seq(groupA), Seq(aggCountB))
        // All having conditions should be preserved
        .having(groupA.attr > 1 and aggCountB.attr < 3L)
        .select(aggCountB.attr as "COUNT(b)")
    )
  }

  test("aggregate with multiple alternate having and order by clauses") {
    val analyzedPlan = analyze {
      relation
        .groupBy('a).agg('a)
        .having('a > 1)
        .orderBy('a.asc)
        .having(count('b) < 10L)
        .orderBy(count('b).asc)
    }

    val expectedPlan =
      Aggregate(relation, Seq(groupA), Seq(aggCountB))
        .having(groupA.attr > 1 and (aggCountB.attr < 10L))
        .orderBy(aggCountB.attr.asc)
        .select(groupA.attr as 'a)

    checkPlan(analyzedPlan, expectedPlan)
  }

  test("analyzed aggregate should not expose `GeneratedAttribute`s") {
    checkPlan(
      // The "a" in agg list will be replaced by a `GroupingAttribute` during resolution.  This
      // `GroupingAttribute` must be aliased to the original name in the final analyzed plan.
      analyze(relation groupBy 'a agg 'a),
      Aggregate(relation, Seq(groupA), Nil) select (groupA.attr as 'a)
    )
  }
}
