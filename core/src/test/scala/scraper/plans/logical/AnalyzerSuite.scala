package scraper.plans.logical

import org.scalatest.BeforeAndAfterAll
import scraper.expressions.Expression
import scraper.expressions.NamedExpression.newExpressionID
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.parser.Parser
import scraper.plans.logical.dsl._
import scraper.utils.quote
import scraper.{InMemoryCatalog, LoggingFunSuite, Test, TestUtils}

class AnalyzerSuite extends LoggingFunSuite with TestUtils with BeforeAndAfterAll {
  private val catalog = new InMemoryCatalog

  private val analyze = new Analyzer(catalog)

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val (groupA, groupB, aggCountA, aggCountB) =
    (a.asGrouping, b.asGrouping, count(a).asAgg, count(b).asAgg)

  private val relation = LocalRelation.empty(a, b)

  override protected def beforeAll(): Unit = {
    catalog.registerRelation("t", relation)
  }

  testAlias('a + 1, "(a + 1)")

  testAlias($"t.a" + 1, "(a + 1)")

  testAlias(lit("foo"), "foo")

  test("resolve references") {
    checkAnalyzedPlan(
      relation select ('b, ('a + 1) as 's),
      relation select (b, (a + 1) as 's)
    )
  }

  test("expand stars") {
    checkAnalyzedPlan(
      relation select '*,
      relation select (a, b)
    )
  }

  test("sql - expand stars") {
    checkAnalyzedPlan(
      "select * from t",
      relation as 't select (a qualifiedBy 't, b qualifiedBy 't)
    )
  }

  test("expand stars with qualifier") {
    checkAnalyzedPlan(
      relation as 'x join (relation as 'y) select $"x.*",
      relation as 'x join (relation.newInstance() as 'y) select (a qualifiedBy 'x, b qualifiedBy 'x)
    )
  }

  test("self-join") {
    checkAnalyzedPlan(
      relation join relation,
      relation join relation.newInstance()
    )
  }

  test("duplicated aliases") {
    val alias = 1 as 'a
    val newAlias = alias withID newExpressionID()

    checkAnalyzedPlan(
      SingleRowRelation select alias union (SingleRowRelation select alias),
      SingleRowRelation select alias union (SingleRowRelation select newAlias)
    )
  }

  test("global aggregate") {
    checkAnalyzedPlan(
      relation select count('a),
      Aggregate(relation, Nil, Seq(aggCountA)) select (aggCountA.attr as "COUNT(a)")
    )
  }

  test("aggregate with both having and order by clauses") {
    checkAnalyzedPlan(
      relation groupBy 'a agg 'a having 'a > 1 orderBy count('b).asc,
      Aggregate(relation, Seq(groupA), Seq(aggCountB))
        .having(groupA.attr > 1)
        .orderBy(aggCountB.attr.asc)
        .select(groupA.attr as 'a)
    )
  }

  test("aggregate with multiple order by clauses") {
    checkAnalyzedPlan(
      relation groupBy 'a agg count('b) orderBy 'a.asc orderBy count('b).asc,
      Aggregate(relation, Seq(groupA), Seq(aggCountB))
        // Only the last sort order should be preserved
        .orderBy(aggCountB.attr.asc)
        .select(aggCountB.attr as "COUNT(b)")
    )
  }

  test("aggregate with multiple having conditions") {
    checkAnalyzedPlan(
      relation groupBy 'a agg count('b) having 'a > 1 having count('b) < 3L,
      Aggregate(relation, Seq(groupA), Seq(aggCountB))
        // All having conditions should be preserved
        .having(groupA.attr > 1 and aggCountB.attr < 3L)
        .select(aggCountB.attr as "COUNT(b)")
    )
  }

  test("aggregate with multiple alternate having and order by clauses") {
    val plan =
      relation
        .groupBy('a).agg('a)
        .having('a > 1)
        .orderBy('a.asc)
        .having(count('b) < 10L)
        .orderBy(count('b).asc)

    val expectedPlan =
      Aggregate(relation, Seq(groupA), Seq(aggCountB))
        .having(groupA.attr > 1 and (aggCountB.attr < 10L))
        .orderBy(aggCountB.attr.asc)
        .select(groupA.attr as 'a)

    checkAnalyzedPlan(plan, expectedPlan)
  }

  test("analyzed aggregate should not expose `GeneratedAttribute`s") {
    checkAnalyzedPlan(
      // The "a" in agg list will be replaced by a `GroupingAttribute` during resolution.  This
      // `GroupingAttribute` must be aliased to the original name in the final analyzed plan.
      relation groupBy 'a agg 'a,
      Aggregate(relation, Seq(groupA), Nil) select (groupA.attr as 'a)
    )
  }

  test("distinct") {
    checkAnalyzedPlan(
      relation.distinct,
      Aggregate(relation, Seq(groupA, groupB), Nil) select (groupA.attr as 'a, groupB.attr as 'b)
    )
  }

  private def checkAnalyzedPlan(sql: String, expected: LogicalPlan): Unit =
    checkAnalyzedPlan(new Parser(Test.defaultSettings) parse sql, expected)

  private def checkAnalyzedPlan(unresolved: LogicalPlan, expected: LogicalPlan): Unit =
    checkPlan(analyze(unresolved), expected)

  private def testAlias(expression: Expression, expectedAlias: String): Unit = {
    test(s"auto-alias resolution - $expression AS ${quote(expectedAlias)}") {
      val Seq(actualAlias) = analyze(relation as 't select expression).output map (_.name)
      assert(actualAlias == expectedAlias)
    }
  }
}
