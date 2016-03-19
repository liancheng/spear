package scraper.parser

import scraper.{LoggingFunSuite, Test, TestUtils}
import scraper.expressions.dsl._
import scraper.plans.logical.{LogicalPlan, SingleRowRelation, UnresolvedRelation}
import scraper.plans.logical.dsl._

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
    "SELECT 1",
    singleRow select 1
  )

  testParser(
    "SELECT 1 AS a FROM t0",
    t0 select (1 as 'a)
  )

  testParser(
    "SELECT * FROM t0",
    t0 select '*
  )

  testParser(
    "SELECT a.* FROM t0 a",
    t0 as 'a select $"a.*"
  )

  testParser(
    "SELECT * FROM t0, t1",
    t0 join t1 select '*
  )

  testParser(
    "SELECT * FROM t0 a JOIN t1 b",
    t0 as 'a join (t1 as 'b) select '*
  )

  testParser(
    "SELECT a.* FROM t0 a JOIN t1 b",
    t0 as 'a join (t1 as 'b) select $"a.*"
  )

  testParser(
    "SELECT t.a FROM (SELECT * FROM t0) t",
    t0 select '* as 't select $"t.a"
  )
}
