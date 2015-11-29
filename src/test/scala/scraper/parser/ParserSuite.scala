package scraper.parser

import scraper.LoggingFunSuite
import scraper.expressions.functions.lit
import scraper.plans.logical.UnresolvedRelation
import scraper.types.TestUtils

class ParserSuite extends LoggingFunSuite with TestUtils {
  test("simple query") {
    checkPlan(
      new Parser().parse("SELECT 1 AS a FROM t"),
      UnresolvedRelation("t") select (lit(1) as 'a)
    )
  }
}
