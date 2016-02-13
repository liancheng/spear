package scraper.parser

import scraper.expressions.dsl._
import scraper.plans.logical.{LogicalPlan, SingleRowRelation, UnresolvedRelation}
import scraper.{LoggingFunSuite, Test, TestUtils}

class ParserSuite extends LoggingFunSuite with TestUtils {
  private def testParser(sql: String, expectedPlan: LogicalPlan): Unit = {
    test(sql) {
      checkPlan(new Parser(Test.defaultSettings) parse sql, expectedPlan)
    }
  }

  private val t0 = UnresolvedRelation("t0")

  private val t1 = UnresolvedRelation("t1")

  private val singleRow = SingleRowRelation

  testParser(
    "SELECT 1 AS a FROM t0",
    t0 select (1 as 'a)
  )

  testParser(
    "SELECT 1",
    singleRow select (1 as 'col0)
  )

  testParser(
    "SELECT * FROM t0, t1",
    t0 join t1 select '*
  )
}
