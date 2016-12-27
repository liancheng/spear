package scraper.fastparser

import scraper.{LoggingFunSuite, TestUtils}
import scraper.expressions._
import scraper.plans.logical._

class QuerySpecificationParserSuite extends LoggingFunSuite with TestUtils {
  import fastparse.all._

  testQueryParsing(
    "SELECT 1",
    values(1)
  )

  testQueryParsing(
    "SELECT 1 AS a FROM t0",
    table('t0) select (1 as 'a)
  )

  testQueryParsing(
    "SELECT * FROM t0",
    table('t0) select *
  )

  testQueryParsing(
    "SELECT a.* FROM t0 a",
    table('t0) subquery 'a select $"a.*"
  )

  testQueryParsing(
    "SELECT a FROM t0 WHERE a > 10",
    table('t0) filter 'a > 10 select 'a
  )

  private def testQueryParsing(sql: String, expectedPlan: LogicalPlan): Unit = {
    test(s"parsing SQL: $sql") {
      checkPlan(parse(sql.split("\n").map(_.trim).mkString(" ")), expectedPlan)
    }
  }

  private def parse(input: String): LogicalPlan = {
    (Start ~ QuerySpecificationParser.querySpecification ~ End).parse(input.trim).get.value
  }
}
