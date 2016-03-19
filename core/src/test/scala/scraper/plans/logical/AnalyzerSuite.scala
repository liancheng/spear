package scraper.plans.logical

import org.scalatest.BeforeAndAfterAll

import scraper._
import scraper.exceptions.{IllegalAggregationException, ResolutionFailureException}
import scraper.expressions._
import scraper.expressions.NamedExpression.newExpressionID
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.parser.Parser
import scraper.plans.logical.AnalyzerSuite.NonSQL
import scraper.plans.logical.dsl._
import scraper.types.{DataType, NullType}
import scraper.utils.quote

class AnalyzerSuite extends LoggingFunSuite with TestUtils with BeforeAndAfterAll {
  private val catalog = new InMemoryCatalog

  private val analyze = new Analyzer(catalog)

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val `t.a` = a qualifiedBy 't

  private val `t.b` = b qualifiedBy 't

  private val (groupA, groupB, aggCountA, aggCountB) =
    (a.asGrouping, b.asGrouping, count(a).asAgg, count(b).asAgg)

  private val relation = LocalRelation.empty(a, b)

  override protected def beforeAll(): Unit = {
    catalog.registerRelation("t", relation)
  }

  testAlias('a + 1, "(a + 1)")

  testAlias($"t.a" + 1, "(a + 1)")

  testAlias(lit("foo"), "foo")

  testAlias(NonSQL, "?column?")

  test("resolve references") {
    checkAnalyzedPlan(
      relation select (('a + 1) as 's),
      relation select ((a + 1) as 's)
    )
  }

  test("resolve references in SQL") {
    checkAnalyzedPlan(
      "SELECT a + 1 AS s FROM t",
      relation as 't select ((`t.a` + 1) as 's)
    )
  }

  test("resolve qualified references") {
    checkAnalyzedPlan(
      relation as 't select (($"t.a" + 1) as 's),
      relation as 't select ((`t.a` + 1) as 's)
    )
  }

  test("resolve qualified references in SQL") {
    checkAnalyzedPlan(
      "SELECT t.a + 1 AS s FROM t",
      relation as 't select ((`t.a` + 1) as 's)
    )
  }

  test("non-existed reference") {
    intercept[ResolutionFailureException] {
      analyze(relation select 'bad)
    }
  }

  test("ambiguous reference") {
    intercept[ResolutionFailureException] {
      analyze(LocalRelation.empty(a, a withID newExpressionID()) select 'a)
    }
  }

  test("no generated output allowed") {
    intercept[ResolutionFailureException] {
      analyze(LocalRelation.empty('a.int.!.asGrouping.attr))
    }
  }

  test("expand stars") {
    checkAnalyzedPlan(
      relation select '*,
      relation select (a, b)
    )
  }

  test("expand stars in SQL") {
    checkAnalyzedPlan(
      "SELECT * FROM t",
      relation as 't select (`t.a`, `t.b`)
    )
  }

  test("expand stars with qualifier") {
    val `x.a` = a qualifiedBy 'x
    val `x.b` = b qualifiedBy 'x

    checkAnalyzedPlan(
      relation as 'x join (relation as 'y) select $"x.*",
      relation as 'x join (relation.newInstance() as 'y) select (`x.a`, `x.b`)
    )
  }

  test("expand stars with qualifier in SQL") {
    val `x.a` = a qualifiedBy 'x
    val `x.b` = b qualifiedBy 'x

    checkAnalyzedPlan(
      "SELECT x.* FROM t x JOIN t y",
      relation as 't as 'x join (relation.newInstance() as 't as 'y) select (`x.a`, `x.b`)
    )
  }

  test("self-join") {
    checkAnalyzedPlan(
      relation join relation,
      relation join relation.newInstance()
    )
  }

  test("self-join in SQL") {
    val `t.a'` = a withID newExpressionID() qualifiedBy 't
    val `t.b'` = b withID newExpressionID() qualifiedBy 't

    checkAnalyzedPlan(
      "SELECT * FROM t JOIN t",
      relation as 't join (relation.newInstance() as 't) select (`t.a`, `t.b`, `t.a'`, `t.b'`)
    )
  }

  test("duplicated aliases") {
    val alias = 1 as 'a
    val newAlias = alias withID newExpressionID()

    checkAnalyzedPlan(
      SingleRowRelation select alias union (SingleRowRelation select alias),
      SingleRowRelation select alias union (SingleRowRelation select newAlias)
    )

    checkAnalyzedPlan(
      SingleRowRelation select alias intersect (SingleRowRelation select alias),
      SingleRowRelation select alias intersect (SingleRowRelation select newAlias)
    )

    checkAnalyzedPlan(
      SingleRowRelation select alias except (SingleRowRelation select alias),
      SingleRowRelation select alias except (SingleRowRelation select newAlias)
    )
  }

  test("global aggregate") {
    checkAnalyzedPlan(
      relation select count('a),
      Aggregate(relation, Nil, Seq(aggCountA)) select (aggCountA.attr as "COUNT(a)")
    )
  }

  test("global aggregate in SQL") {
    val aggCountA = count(`t.a`).asAgg

    checkAnalyzedPlan(
      "SELECT COUNT(a) FROM t",
      Aggregate(relation as 't, Nil, Seq(aggCountA)) select (aggCountA.attr as "COUNT(a)")
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

  test("illegal aggregation") {
    intercept[IllegalAggregationException] {
      analyze(relation groupBy 'a agg 'b)
    }
  }

  test("distinct") {
    checkAnalyzedPlan(
      relation.distinct,
      Aggregate(relation, Seq(groupA, groupB), Nil) select (groupA.attr as 'a, groupB.attr as 'b)
    )
  }

  test("order by columns not appearing in project list") {
    checkAnalyzedPlan(
      relation select 'b orderBy (('a + 1).asc, 'b.desc),
      relation select (b, a) orderBy ((a + 1).asc, b.desc) select b
    )
  }

  test("order by columns not appearing in project list in SQL") {
    checkAnalyzedPlan(
      "SELECT b FROM t ORDER BY a + 1 ASC, b DESC",
      relation as 't select (`t.b`, `t.a`) orderBy ((`t.a` + 1).asc, `t.b`.desc) select `t.b`
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

object AnalyzerSuite {
  case object NonSQL extends NonSQLExpression with LeafExpression with UnevaluableExpression {
    override def dataType: DataType = NullType
  }
}
