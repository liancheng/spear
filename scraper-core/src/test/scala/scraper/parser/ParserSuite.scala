package scraper.parser

import scraper.{LoggingFunSuite, TestUtils}
import scraper.exceptions.ParsingException
import scraper.expressions._
import scraper.plans.logical._
import scraper.plans.logical.LogicalPlan
import scraper.types._

class ParserSuite extends LoggingFunSuite with TestUtils {
  testDataTypeParsing("ARRAY<INT>", ArrayType(IntType.?))

  testDataTypeParsing("MAP<INT, STRING>", MapType(IntType, StringType.?))

  testDataTypeParsing(
    "STRUCT<name: STRING, age: INT>",
    StructType(
      'name -> StringType.?,
      'age -> IntType.?
    )
  )

  testDataTypeParsing(
    "STRUCT<`name`: STRING, `age`: INT>",
    StructType(
      'name -> StringType.?,
      'age -> IntType.?
    )
  )

  testExpressionParsing("a IN (1, 2, 3)", 'a in (1, 2, 3))

  testExpressionParsing("a RLIKE 'a*'", 'a rlike "a*")

  testExpressionParsing(
    "a RLIKE concat('h.llo', ' worl.')",
    'a rlike 'concat("h.llo", " worl.")
  )

  test("invalid query") {
    intercept[ParsingException] {
      parse("garbage")
    }
  }

  test("DISTINCT can't be used with star") {
    intercept[ParsingException] {
      parse("SELECT COUNT(DISTINCT *) FROM t0")
    }
  }

  test("unclosed single-quoted string literal") {
    intercept[ParsingException] {
      parse("SELECT 'hello")
    }
  }

  test("unclosed double-quoted string literal") {
    intercept[ParsingException] {
      parse("SELECT \"hello")
    }
  }

  test("unclosed multi-line comment") {
    intercept[ParsingException] {
      parse("SELECT 1 /* comment")
    }
  }

  private def testDataTypeParsing(sql: String, dataType: DataType): Unit = {
    test(s"parsing data type: $sql") {
      checkPlan(
        parse(s"SELECT CAST(a AS $sql) FROM t0"),
        table('t0) select ('a cast dataType)
      )
    }
  }

  private def testExpressionParsing(sql: String, expression: Expression): Unit = {
    test(s"parsing expression: $sql") {
      checkPlan(
        parse(s"SELECT $sql FROM t0"),
        table('t0) select expression
      )
    }
  }

  private def parse(query: String): LogicalPlan = new Parser parse query
}
