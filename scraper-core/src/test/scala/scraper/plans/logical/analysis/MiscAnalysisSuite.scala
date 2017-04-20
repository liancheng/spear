package scraper.plans.logical.analysis

import scraper._
import scraper.expressions._
import scraper.expressions.NamedExpression.newExpressionID
import scraper.plans.logical._

class MiscAnalysisSuite extends AnalyzerTest {
  test("self-join") {
    checkAnalyzedPlan(
      relation join relation,
      relation join relation.newInstance()
    )
  }

  test("self-join in SQL") {
    val relation0 = relation.newInstance()
    val Seq(a0: AttributeRef, b0: AttributeRef) = relation0.output

    checkAnalyzedPlan(
      "SELECT * FROM t JOIN t",
      relation
        subquery 't
        join (relation0 subquery 't)
        select (a of 't, b of 't, a0 of 't, b0 of 't)
    )
  }

  test("duplicated aliases") {
    val alias = 1 as 'a
    val newAlias = alias withID newExpressionID()

    checkAnalyzedPlan(
      SingleRowRelation() select alias union (SingleRowRelation() select alias),
      SingleRowRelation() select alias union (SingleRowRelation() select newAlias)
    )

    checkAnalyzedPlan(
      SingleRowRelation() select alias intersect (SingleRowRelation() select alias),
      SingleRowRelation() select alias intersect (SingleRowRelation() select newAlias)
    )

    checkAnalyzedPlan(
      SingleRowRelation() select alias except (SingleRowRelation() select alias),
      SingleRowRelation() select alias except (SingleRowRelation() select newAlias)
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
      relation
        subquery 't
        select (b of 't, a of 't)
        orderBy (((a of 't) + 1).asc, (b of 't).desc)
        select (b of 't)
    )
  }

  override protected def beforeAll(): Unit = catalog.registerRelation('t, relation)

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val relation = LocalRelation.empty(a, b)
}
