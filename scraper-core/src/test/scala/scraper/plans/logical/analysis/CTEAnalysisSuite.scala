package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.AnalysisException
import scraper.expressions._
import scraper.expressions.NamedExpression.newExpressionID
import scraper.plans.logical._

class CTEAnalysisSuite extends AnalyzerTest {
  test("CTE") {
    checkAnalyzedPlan(
      let('s, relation0 subquery 't select 'a) {
        table('s) select *
      },
      relation0 subquery 't select (a of 't) subquery 's select (a of 's)
    )
  }

  test("CTE with aliases") {
    checkAnalyzedPlan(
      let('s, relation0 subquery 't, Seq('x, 'y)) {
        table('s) select *
      },
      relation0
        subquery 't
        select (a of 't as 'x, b of 't as 'y)
        subquery 's
        select (x of 's, y of 's)
    )
  }

  test("CTE with fewer aliases") {
    checkAnalyzedPlan(
      let('s, relation0 select *, Seq('x)) {
        table('s)
      },
      relation0
        select (a, b)
        select (a as 'x, b)
        subquery 's
    )
  }

  test("CTE with excessive aliases") {
    intercept[AnalysisException] {
      analyze {
        let('s, relation0 select *, Seq('x, 'y, 'z)) {
          table('s)
        }
      }
    }
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
        |""".stripMargin,
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

  override protected def beforeAll(): Unit = catalog.registerRelation('t, relation0)

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val (x, y) = ('x.int.!, 'y.string.?)

  private val relation0 = LocalRelation.empty(a, b)

  private val (c, d) = ('c.int.!, 'd.string.?)

  private val relation1 = LocalRelation.empty(c, d)
}
