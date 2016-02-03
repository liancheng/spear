package scraper.plans.logical

import scraper.expressions.dsl._
import scraper.{LocalCatalog, LoggingFunSuite, TestUtils}

class AnalyzerSuite extends LoggingFunSuite with TestUtils {
  private val analyzer = new Analyzer(new LocalCatalog)

  test("resolve references") {
    val relation = LocalRelation.empty('a.int.!, 'b.string.?)

    checkPlan(
      analyzer.resolve(relation select ('b, ('a + 1) as 's)),
      relation select ('b.string.?, ('a.int.! + 1) as 's)
    )
  }

  test("expand stars") {
    val relation = LocalRelation.empty('a.int.!, 'b.string.?)

    checkPlan(
      analyzer.resolve(relation select '*),
      relation select ('a.int.!, 'b.string.?)
    )
  }

  test("self-join") {
    val relation = LocalRelation.empty('a.int.!)

    checkPlan(
      analyzer.resolve(relation join relation),
      relation join relation.newInstance()
    )
  }
}
