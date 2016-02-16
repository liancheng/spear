package scraper.plans.logical

import scraper.expressions.dsl._
import scraper.local.LocalCatalog
import scraper.{LoggingFunSuite, TestUtils}

class AnalyzerSuite extends LoggingFunSuite with TestUtils {
  private val analyzer = new Analyzer(new LocalCatalog)

  test("resolve references") {
    val relation = LocalRelation.empty('a.int.!, 'b.string.?)

    checkPlan(
      analyzer resolve (relation select ('b, ('a + 1) as 's)),
      relation select ('b.string.?, ('a.int.! + 1) as 's)
    )
  }

  test("expand stars") {
    val relation = LocalRelation.empty('a.int.!, 'b.string.?)

    checkPlan(
      analyzer resolve (relation select '*),
      relation select ('a.int.!, 'b.string.?)
    )
  }

  test("self-join") {
    val relation = LocalRelation.empty('a.int.!)

    checkPlan(
      analyzer resolve (relation join relation),
      relation join relation.newInstance()
    )
  }

  test("duplicated aliases") {
    val plan = analyzer resolve (SingleRowRelation select (1 as 'a))

    checkPlan(
      analyzer resolve (plan union plan),
      // Not that the following two aliases have different expression IDs.
      SingleRowRelation select (1 as 'a) union (SingleRowRelation select (1 as 'a))
    )
  }
}
