package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.{AnalysisException, ResolutionFailureException}
import scraper.expressions._
import scraper.expressions.aggregates._
import scraper.expressions.functions._
import scraper.expressions.NamedExpression.newExpressionID
import scraper.plans.logical.{LocalRelation, Project}
import scraper.plans.logical.analysis.ExpressionAnalysisSuite.NonSQL
import scraper.types.{DataType, NullType}

class ExpressionAnalysisSuite extends AnalyzerTest {
  testAutoAliasing('a + 1, "(a + 1)")

  // Case-insensitive name resolution
  testAutoAliasing('B + 1, "(b + 1)")

  testAutoAliasing($"t.a" + 1, "(a + 1)")

  testAutoAliasing(lit("foo"), "foo")

  testAutoAliasing(NonSQL, "?column?")

  testFunctionResolution(
    'count(*),
    Count(1)
  )

  testFunctionResolution(
    'COUNT(*),
    Count(1)
  )

  testFunctionResolution(
    'sum(1).distinct,
    Sum(1).distinct
  )

  testFunctionResolution(
    'concat("1", "2"),
    Concat(Seq("1", "2"))
  )

  interceptFunctionResolution[AnalysisException](
    'count(*).distinct,
    "DISTINCT cannot be used together with star"
  )

  interceptFunctionResolution[AnalysisException](
    'foo(*).distinct,
    "DISTINCT cannot be used together with star"
  )

  interceptFunctionResolution[AnalysisException](
    'foo(*),
    "Only function \"count\" may have star as argument"
  )

  interceptFunctionResolution[AnalysisException](
    'coalesce('a.int.!).distinct,
    "Cannot decorate function coalesce with DISTINCT since it is not an aggregate function"
  )

  test("resolve references") {
    checkAnalyzedPlan(
      relation select (('a + 1) as 's),
      relation select ((a + 1) as 's)
    )
  }

  test("resolve references in SQL") {
    checkAnalyzedPlan(
      "SELECT a + 1 AS s FROM t",
      relation subquery 't select (((a of 't) + 1) as 's)
    )
  }

  test("resolve qualified references") {
    checkAnalyzedPlan(
      relation subquery 't select (($"t.a" + 1) as 's),
      relation subquery 't select (((a of 't) + 1) as 's)
    )
  }

  test("resolve qualified references in SQL") {
    checkAnalyzedPlan(
      "SELECT t.a + 1 AS s FROM t",
      relation subquery 't select (((a of 't) + 1) as 's)
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

  test("expand stars") {
    checkAnalyzedPlan(
      relation select *,
      relation select (a, b)
    )
  }

  test("expand stars in SQL") {
    checkAnalyzedPlan(
      "SELECT * FROM t",
      relation subquery 't select (a of 't, b of 't)
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

  override protected def beforeAll(): Unit = catalog.registerRelation('t, relation)

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val relation = LocalRelation.empty(a, b)

  private def testAutoAliasing(expression: Expression, expectedAlias: Name): Unit = {
    test(s"auto-alias resolution - ${expression.sqlLike} AS ${expectedAlias.toString}") {
      val Seq(actualAlias) = analyze(relation subquery 't select expression).output map (_.name)
      assert(actualAlias == expectedAlias)
    }
  }

  private def testFunctionResolution(unresolved: Expression, expected: => Expression): Unit = {
    test(s"function resolution - ${unresolved.sqlLike} to ${expected.sqlLike}") {
      val analyzed = new ResolveFunctions(catalog).apply(relation select unresolved)
      val actual = analyzed match { case _ Project Seq(AutoAlias(resolved)) => resolved }
      assert(actual == expected)
    }
  }

  private def interceptFunctionResolution[T <: AnalysisException: Manifest](
    unresolved: UnresolvedFunction, message: String
  ): Unit = {
    test(s"function resolution - ${unresolved.sqlLike} should fail analyzer") {
      val cause = intercept[T] { analyze(relation select unresolved) }
      assert(cause.getMessage contains message)
    }
  }
}

object ExpressionAnalysisSuite {
  case object NonSQL extends NonSQLExpression with LeafExpression with UnevaluableExpression {
    override def dataType: DataType = NullType
  }
}
