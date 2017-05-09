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
    val `@S: a + 1` = SortOrderAlias(a + 1, "order0")
    val `@S: a * 2` = SortOrderAlias(a * 2, "order1")

    checkAnalyzedPlan(
      relation
        select 'b
        orderBy (('a + 1).asc, ('a * 2).desc, 'b.desc),

      relation
        select (b, `@S: a + 1`, `@S: a * 2`)
        sort (`@S: a + 1`.attr.asc, `@S: a * 2`.attr.desc, b.desc)
        select b
    )
  }

  test("order by columns not appearing in project list in SQL") {
    val `@S: a + 1` = SortOrderAlias((a of 't) + 1, "order0")
    val `@S: a * 2` = SortOrderAlias((a of 't) * 2, "order1")

    checkAnalyzedPlan(
      "SELECT b FROM t ORDER BY a + 1 ASC, a * 2 DESC, b DESC",

      relation
        subquery 't
        select (b of 't, `@S: a + 1`, `@S: a * 2`)
        sort (`@S: a + 1`.attr.asc, `@S: a * 2`.attr.desc, (b of 't).desc)
        select (b of 't)
    )
  }

  override protected def beforeAll(): Unit = catalog.registerRelation('t, relation)

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val relation = LocalRelation.empty(a, b)
}
