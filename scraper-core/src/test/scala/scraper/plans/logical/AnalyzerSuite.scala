package scraper.plans.logical

import org.scalatest.BeforeAndAfterAll

import scraper._
import scraper.exceptions.{AnalysisException, IllegalAggregationException, ResolutionFailureException}
import scraper.expressions._
import scraper.expressions.NamedExpression.newExpressionID
import scraper.expressions.aggregates.{Count, Sum}
import scraper.expressions.functions._
import scraper.parser.Parser
import scraper.plans.logical.AnalyzerSuite.NonSQL
import scraper.plans.logical.analysis._
import scraper.types.{DataType, NullType}
import scraper.utils._

class AnalyzerSuite extends LoggingFunSuite with TestUtils with BeforeAndAfterAll {
  private val catalog = new InMemoryCatalog

  private val analyze = new Analyzer(catalog)

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val (c, d) = ('c.int.!, 'd.string.?)

  private val (groupA, groupB, aggCountA, aggCountB, aggCount1) = (
    GroupingAlias(a),
    GroupingAlias(b),
    AggregationAlias(count(a)),
    AggregationAlias(count(b)),
    AggregationAlias(count(1))
  )

  private val relation0 = LocalRelation.empty(a, b)

  private val relation1 = LocalRelation.empty(c, d)

  override protected def beforeAll(): Unit = {
    catalog.registerRelation('t, relation0)
  }

  testAutoAliasing('a + 1, "(a + 1)")

  // Case-insensitive name resolution
  testAutoAliasing('B + 1, "(b + 1)")

  testAutoAliasing($"t.a" + 1, "(a + 1)")

  testAutoAliasing(lit("foo"), "foo")

  testAutoAliasing(NonSQL, "?column?")

  testFunctionResolution(
    function('count, *),
    Count(1)
  )

  testFunctionResolution(
    function('COUNT, *),
    Count(1)
  )

  testFunctionResolution(
    distinctFunction('sum, 1),
    Sum(1).distinct
  )

  testFunctionResolution(
    function('concat, "1", "2"),
    Concat(Seq("1", "2"))
  )

  interceptFunctionResolution[AnalysisException](
    distinctFunction('count, *),
    "DISTINCT cannot be used together with star"
  )

  interceptFunctionResolution[AnalysisException](
    distinctFunction('foo, *),
    "DISTINCT cannot be used together with star"
  )

  interceptFunctionResolution[AnalysisException](
    function('foo, *),
    "Only function \"count\" may have star as argument"
  )

  interceptFunctionResolution[AnalysisException](
    distinctFunction('coalesce, 'a.int.!),
    "Cannot decorate function coalesce with DISTINCT since it is not an aggregate function"
  )

  test("resolve references") {
    checkAnalyzedPlan(
      relation0 select (('a + 1) as 's),
      relation0 select ((a + 1) as 's)
    )
  }

  test("resolve references in SQL") {
    checkAnalyzedPlan(
      "SELECT a + 1 AS s FROM t",
      relation0 subquery 't select (((a of 't) + 1) as 's)
    )
  }

  test("resolve qualified references") {
    checkAnalyzedPlan(
      relation0 subquery 't select (($"t.a" + 1) as 's),
      relation0 subquery 't select (((a of 't) + 1) as 's)
    )
  }

  test("resolve qualified references in SQL") {
    checkAnalyzedPlan(
      "SELECT t.a + 1 AS s FROM t",
      relation0 subquery 't select (((a of 't) + 1) as 's)
    )
  }

  test("non-existed reference") {
    intercept[ResolutionFailureException] {
      analyze(relation0 select 'bad)
    }
  }

  test("ambiguous reference") {
    intercept[ResolutionFailureException] {
      analyze(LocalRelation.empty(a, a withID newExpressionID()) select 'a)
    }
  }

  test("expand stars") {
    checkAnalyzedPlan(
      relation0 select *,
      relation0 select (a, b)
    )
  }

  test("expand stars in SQL") {
    checkAnalyzedPlan(
      "SELECT * FROM t",
      relation0 subquery 't select (a of 't, b of 't)
    )
  }

  test("expand stars with qualifier") {
    checkAnalyzedPlan(
      relation0 subquery 'x join (relation0 subquery 'y) select $"x.*",
      relation0 subquery 'x join (relation0.newInstance() subquery 'y) select (a of 'x, b of 'x)
    )
  }

  test("expand stars with qualifier in SQL") {
    checkAnalyzedPlan(
      "SELECT x.* FROM t x JOIN t y",
      relation0 subquery 't subquery 'x
        join (relation0.newInstance() subquery 't subquery 'y)
        select (a of 'x, b of 'x)
    )
  }

  test("self-join") {
    checkAnalyzedPlan(
      relation0 join relation0,
      relation0 join relation0.newInstance()
    )
  }

  test("self-join in SQL") {
    val a0 = a withID newExpressionID()
    val b0 = b withID newExpressionID()

    checkAnalyzedPlan(
      "SELECT * FROM t JOIN t",
      relation0
        subquery 't
        join (relation0.newInstance() subquery 't)
        select (a of 't, b of 't, a0 of 't, b0 of 't)
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

  test("order by columns not appearing in project list") {
    checkAnalyzedPlan(
      relation0 select 'b orderBy (('a + 1).asc, 'b.desc),
      relation0 select (b, a) orderBy ((a + 1).asc, b.desc) select b
    )
  }

  test("order by columns not appearing in project list in SQL") {
    checkAnalyzedPlan(
      "SELECT b FROM t ORDER BY a + 1 ASC, b DESC",
      relation0
        subquery 't
        select (b of 't, a of 't)
        orderBy (((a of 't) + 1).asc, (b of 't).desc)
        select (b of 't)
    )
  }

  test("CTE") {
    checkAnalyzedPlan(
      let('s, relation0 subquery 't select 'a) {
        table('s) select *
      },
      relation0 subquery 't select (a of 't) subquery 's select (a of 's)
    )
  }

  test("CTE in SQL") {
    checkAnalyzedPlan(
      "WITH s AS (SELECT a FROM t) SELECT * FROM s",
      relation0 subquery 't select (a of 't) subquery 's select (a of 's)
    )
  }

  test("multiple CTE") {
    checkAnalyzedPlan(
      let('s0, relation0 subquery 't) {
        let('s1, relation0 subquery 't) {
          table('s0) union table('s1)
        }
      },
      relation0 subquery 't subquery 's0 union (
        relation0.newInstance() subquery 't subquery 's1
      )
    )
  }

  test("multiple CTE in SQL") {
    val x0 = 'x.int.!
    val x1 = x0 withID newExpressionID()

    checkAnalyzedPlan(
      """WITH
        |  s0 AS (SELECT 1 AS x),
        |  s1 AS (SELECT 2 AS x)
        |SELECT *
        |FROM s0 UNION ALL SELECT * FROM s1
      """.oneLine,
      values(1 as 'x) subquery 's0 select (x0 of 's0) union (
        values(2 as 'x) subquery 's1 select (x1 of 's1)
      )
    )
  }

  test("nested CTE") {
    checkAnalyzedPlan(
      let('s, relation0 subquery 't0) {
        table('s) union let('s, relation1 subquery 't1) {
          table('s) select ('c as 'a, 'd as 'b)
        }
      },
      relation0 subquery 't0 subquery 's union (
        relation1 subquery 't1 subquery 's select (c of 's as 'a, d of 's as 'b)
      )
    )
  }

  test("post-analysis check - reject unresolved expressions") {
    val rule = new RejectUnresolvedExpressions(catalog)

    intercept[ResolutionFailureException] {
      rule(relation0 select 'a)
    }
  }

  test("post-analysis check - reject unresolved plans") {
    val rule = new RejectUnresolvedPlans(catalog)

    intercept[ResolutionFailureException] {
      rule(relation0 agg (1 as 'a))
    }
  }

  test("post-analysis check - reject generated attributes") {
    val rule = new RejectGeneratedAttributes(catalog)

    intercept[ResolutionFailureException] {
      rule(LocalRelation.empty(GroupingAlias('a.int.!).attr))
    }
  }

  test("post-analysis check - reject distinct aggregate function") {
    val rule = new RejectDistinctAggregateFunctions(catalog)

    intercept[ResolutionFailureException] {
      rule(relation0 select distinct(count(a)))
    }
  }

  private def checkAnalyzedPlan(sql: String, expected: LogicalPlan): Unit =
    checkAnalyzedPlan(new Parser parse sql, expected)

  private def checkAnalyzedPlan(unresolved: LogicalPlan, expected: LogicalPlan): Unit =
    checkPlan(analyze(unresolved), expected)

  private def testAutoAliasing(expression: Expression, expectedAlias: Name): Unit = {
    test(s"auto-alias resolution - $expression AS ${expectedAlias.toString}") {
      val Seq(actualAlias) = analyze(relation0 subquery 't select expression).output map (_.name)
      assert(actualAlias == expectedAlias)
    }
  }

  private def testFunctionResolution(unresolved: Expression, expected: => Expression): Unit = {
    test(s"function resolution - $unresolved to $expected") {
      val analyzed = new ResolveFunctions(catalog).apply(relation0 select unresolved)
      val actual = analyzed match { case _ Project Seq(AutoAlias(resolved)) => resolved }
      assert(actual == expected)
    }
  }

  private def interceptFunctionResolution[T <: AnalysisException: Manifest](
    unresolved: UnresolvedFunction, message: String
  ): Unit = {
    test(s"function resolution - $unresolved should fail analyzer") {
      val cause = intercept[T] { analyze(relation0 select unresolved) }
      assert(cause.getMessage contains message)
    }
  }
}

object AnalyzerSuite {
  case object NonSQL extends NonSQLExpression with LeafExpression with UnevaluableExpression {
    override def dataType: DataType = NullType
  }
}
