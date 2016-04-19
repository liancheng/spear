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
import scraper.utils._

class AnalyzerSuite extends LoggingFunSuite with TestUtils with BeforeAndAfterAll {
  private val catalog = new InMemoryCatalog

  private val analyze = new Analyzer(catalog)

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val `t.a` = a of 't

  private val `t.b` = b of 't

  private val (groupA, groupB, aggCountA, aggCountB) = (
    GroupingAlias(a),
    GroupingAlias(b),
    AggregationAlias(count(a)),
    AggregationAlias(count(b))
  )

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
      relation subquery 't select ((`t.a` + 1) as 's)
    )
  }

  test("resolve qualified references") {
    checkAnalyzedPlan(
      relation subquery 't select (($"t.a" + 1) as 's),
      relation subquery 't select ((`t.a` + 1) as 's)
    )
  }

  test("resolve qualified references in SQL") {
    checkAnalyzedPlan(
      "SELECT t.a + 1 AS s FROM t",
      relation subquery 't select ((`t.a` + 1) as 's)
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
      analyze(LocalRelation.empty(GroupingAlias('a.int.!).attr))
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
      relation subquery 't select (`t.a`, `t.b`)
    )
  }

  test("expand stars with qualifier") {
    checkAnalyzedPlan(
      relation subquery 'x join (relation subquery 'y) select $"x.*",
      relation subquery 'x join (relation.newInstance() subquery 'y) select (a of 'x, b of 'x)
    )
  }

  test("expand stars with qualifier in SQL") {
    checkAnalyzedPlan(
      "SELECT x.* FROM t x JOIN t y",
      relation subquery 't subquery 'x
        join (relation.newInstance() subquery 't subquery 'y)
        select (a of 'x, b of 'x)
    )
  }

  test("self-join") {
    checkAnalyzedPlan(
      relation join relation,
      relation join relation.newInstance()
    )
  }

  test("self-join in SQL") {
    val `t.a'` = a withID newExpressionID() of 't
    val `t.b'` = b withID newExpressionID() of 't

    checkAnalyzedPlan(
      "SELECT * FROM t JOIN t",
      relation
        subquery 't
        join (relation.newInstance() subquery 't)
        select (`t.a`, `t.b`, `t.a'`, `t.b'`)
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
      relation resolvedAgg aggCountA select (aggCountA.attr as "COUNT(a)")
    )
  }

  test("global aggregate in SQL") {
    val aggCountA = AggregationAlias(count(`t.a`))

    checkAnalyzedPlan(
      "SELECT COUNT(a) FROM t",
      relation subquery 't resolvedAgg aggCountA select (aggCountA.attr as "COUNT(a)")
    )
  }

  test("aggregate with both having and order by clauses") {
    checkAnalyzedPlan(
      relation groupBy 'a agg 'a having 'a > 1 orderBy count('b).asc,
      relation
        resolvedGroupBy groupA
        agg aggCountB
        having groupA.attr > 1
        orderBy aggCountB.attr.asc
        select (groupA.attr as 'a)
    )
  }

  test("aggregate with multiple order by clauses") {
    checkAnalyzedPlan(
      relation groupBy 'a agg count('b) orderBy 'a.asc orderBy count('b).asc,
      relation
        resolvedGroupBy groupA
        agg aggCountB
        // Only the last sort order should be preserved
        orderBy aggCountB.attr.asc
        select (aggCountB.attr as "COUNT(b)")
    )
  }

  test("aggregate with multiple having conditions") {
    checkAnalyzedPlan(
      relation groupBy 'a agg count('b) having 'a > 1 having count('b) < 3L,
      relation
        resolvedGroupBy groupA
        agg aggCountB
        // All having conditions should be preserved
        having groupA.attr > 1 && aggCountB.attr < 3L
        select (aggCountB.attr as "COUNT(b)")
    )
  }

  test("aggregate with multiple alternate having and order by clauses") {
    checkAnalyzedPlan(
      relation
        groupBy 'a agg 'a
        having 'a > 1
        orderBy 'a.asc
        having count('b) < 10L
        orderBy count('b).asc,
      relation
        resolvedGroupBy groupA
        agg aggCountB
        having groupA.attr > 1 && (aggCountB.attr < 10L)
        orderBy aggCountB.attr.asc
        select (groupA.attr as 'a)
    )
  }

  test("analyzed aggregate should not expose `GeneratedAttribute`s") {
    checkAnalyzedPlan(
      // The "a" in agg list will be replaced by a `GroupingAttribute` during resolution.  This
      // `GroupingAttribute` must be aliased to the original name in the final analyzed plan.
      relation groupBy 'a agg 'a,
      relation resolvedGroupBy groupA agg Nil select (groupA.attr as 'a)
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
      relation
        resolvedGroupBy (groupA, groupB)
        agg Nil
        select (groupA.attr as 'a, groupB.attr as 'b)
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
      relation subquery 't select (`t.b`, `t.a`) orderBy ((`t.a` + 1).asc, `t.b`.desc) select `t.b`
    )
  }

  test("CTE") {
    checkAnalyzedPlan(
      let('s -> (relation subquery 't select 'a)) {
        table('s) select '*
      },
      relation subquery 't select `t.a` subquery 's select (a of 's)
    )
  }

  test("CTE in SQL") {
    checkAnalyzedPlan(
      "WITH s AS (SELECT a FROM t) SELECT * FROM s",
      relation subquery 't select `t.a` subquery 's select (a of 's)
    )
  }

  test("multiple CTE") {
    checkAnalyzedPlan(
      let('s0 -> (relation subquery 't)) {
        let('s1 -> (relation subquery 't)) {
          table('s0) union table('s1)
        }
      },
      relation subquery 't subquery 's0 union (
        relation.newInstance() subquery 't subquery 's1
      )
    )
  }

  test("multiple CTE in SQL") {
    val x0 = 'x.int.!
    val x1 = x0 withID newExpressionID()

    checkAnalyzedPlan(
      """WITH
        |s0 AS (SELECT 1 AS x),
        |s1 AS (SELECT 2 AS x)
        |SELECT * FROM s0 UNION ALL SELECT * FROM s1
      """.straight,
      values(1 as 'x) subquery 's0 select (x0 of 's0) union (
        values(2 as 'x) subquery 's1 select (x1 of 's1)
      )
    )
  }

  private def checkAnalyzedPlan(sql: String, expected: LogicalPlan): Unit =
    checkAnalyzedPlan(new Parser(Test.defaultSettings) parse sql, expected)

  private def checkAnalyzedPlan(unresolved: LogicalPlan, expected: LogicalPlan): Unit =
    checkPlan(analyze(unresolved), expected)

  private def testAlias(expression: Expression, expectedAlias: String): Unit = {
    test(s"auto-alias resolution - $expression AS ${quote(expectedAlias)}") {
      val Seq(actualAlias) = analyze(relation subquery 't select expression).output map (_.name)
      assert(actualAlias == expectedAlias)
    }
  }
}

object AnalyzerSuite {
  case object NonSQL extends NonSQLExpression with LeafExpression with UnevaluableExpression {
    override def dataType: DataType = NullType
  }
}
