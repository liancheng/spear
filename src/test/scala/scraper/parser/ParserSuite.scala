package scraper.parser

import scraper.expressions.dsl._
import scraper.plans.logical.{LogicalPlan, SingleRowRelation, UnresolvedRelation}
import scraper.{LoggingFunSuite, TestUtils}

class ParserSuite extends LoggingFunSuite with TestUtils {
  private def testParser(sql: String, expectedPlan: LogicalPlan): Unit = {
    test(sql) {
      checkPlan(new Parser() parse sql, expectedPlan)
    }
  }

  testParser(
    "SELECT 1 AS a FROM t",
    UnresolvedRelation("t") select (1 as 'a)
  )

  testParser(
    "SELECT 1",
    SingleRowRelation select (1 as 'col0)
  )

  testParser(
    "SELECT * FROM a, b",
    UnresolvedRelation("a") join UnresolvedRelation("b") select '*
  )
}
