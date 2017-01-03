package scraper.parser

import scraper.{LoggingFunSuite, TestUtils}
import scraper.exceptions.ParsingException
import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.logical._
import scraper.plans.logical.LogicalPlan
import scraper.types._

class ParserSuite extends LoggingFunSuite with TestUtils {
  testDataTypeParsing("BOOLEAN", BooleanType)

  testDataTypeParsing("TINYINT", ByteType)

  testDataTypeParsing("SMALLINT", ShortType)

  testDataTypeParsing("INT", IntType)

  testDataTypeParsing("BIGINT", LongType)

  testDataTypeParsing("FLOAT", FloatType)

  testDataTypeParsing("DOUBLE", DoubleType)

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

  testExpressionParsing("1", 1)

  testExpressionParsing(Int.MaxValue.toString, Int.MaxValue)

  testExpressionParsing(Int.MinValue.toString, Int.MinValue)

  testExpressionParsing((Int.MaxValue.toLong + 1).toString, Int.MaxValue.toLong + 1)

  testExpressionParsing((Int.MinValue.toLong - 1).toString, Int.MinValue.toLong - 1)

  testExpressionParsing("\"1\"", "1")

  testExpressionParsing("'1'", "1")

  testExpressionParsing("'hello' ' ' 'world'", "hello world")

  testExpressionParsing("true", true)

  testExpressionParsing("false", false)

  testExpressionParsing("`a`", 'a)

  testExpressionParsing("`a``b`", Symbol("a`b"))

  testExpressionParsing("(a = 1)", 'a === 1)

  testExpressionParsing("a AND b", 'a && 'b)

  testExpressionParsing("a OR b", 'a || 'b)

  testExpressionParsing("NOT a", !'a)

  testExpressionParsing("a = b", 'a === 'b)

  testExpressionParsing("a != b", 'a =/= 'b)

  testExpressionParsing("a <> b", 'a =/= 'b)

  testExpressionParsing("a > b", 'a > 'b)

  testExpressionParsing("a >= b", 'a >= 'b)

  testExpressionParsing("a < b", 'a < 'b)

  testExpressionParsing("a <= b", 'a <= 'b)

  testExpressionParsing("a IS NULL", 'a.isNull)

  testExpressionParsing("a IS NOT NULL", 'a.isNotNull)

  testExpressionParsing("a IN (1, 2, 3)", 'a in (1, 2, 3))

  testExpressionParsing("a RLIKE 'a*'", 'a rlike "a*")

  testExpressionParsing(
    "a RLIKE concat('h.llo', ' worl.')",
    'a rlike 'concat("h.llo", " worl.")
  )

  testExpressionParsing("-a", -'a)

  testExpressionParsing("a + b", 'a + 'b)

  testExpressionParsing("a - b", 'a - 'b)

  testExpressionParsing("a * b", 'a * 'b)

  testExpressionParsing("a / b", 'a / 'b)

  testExpressionParsing("a % b", 'a % 'b)

  testExpressionParsing("a ^ b", 'a ^ 'b)

  testExpressionParsing("a + b * c ^ d", 'a + ('b * ('c ^ 'd)))

  testExpressionParsing(
    "CASE WHEN 1 THEN 'x' WHEN 2 THEN 'y' END",
    when(1, "x") when (2, "y")
  )

  testExpressionParsing(
    "CASE WHEN 1 THEN 'x' WHEN 2 THEN 'y' ELSE 'z' END",
    when(1, "x") when (2, "y") otherwise "z"
  )

  testExpressionParsing(
    "CASE a WHEN 1 THEN 'x' WHEN 2 THEN 'y' END",
    when('a === 1, "x") when ('a === 2, "y")
  )

  testExpressionParsing(
    "CASE a WHEN 1 THEN 'x' WHEN 2 THEN 'y' ELSE 'z' END",
    when('a === 1, "x") when ('a === 2, "y") otherwise "z"
  )

  testExpressionParsing(
    "IF(a > 0, 1, 2)",
    If('a > 0, 1, 2)
  )

  test("invalid query") {
    intercept[ParsingException] {
      parse("garbage")
    }
  }

  testQueryParsing(
    "SELECT * FROM t0 LIMIT 1",
    table('t0) select * limit 1
  )

  testQueryParsing(
    "SELECT * FROM t0 JOIN t1",
    table('t0) join table("t1") select *
  )

  testQueryParsing(
    "SELECT * FROM t0 INNER JOIN t1",
    table('t0) join table("t1") select *
  )

  testQueryParsing(
    "SELECT * FROM t0 LEFT SEMI JOIN t1",
    table('t0) leftSemiJoin table("t1") select *
  )

  testQueryParsing(
    "SELECT * FROM t0 LEFT JOIN t1",
    table('t0) leftJoin table("t1") select *
  )

  testQueryParsing(
    "SELECT * FROM t0 LEFT OUTER JOIN t1",
    table('t0) leftJoin table("t1") select *
  )

  testQueryParsing(
    "SELECT * FROM t0 RIGHT OUTER JOIN t1",
    table('t0) rightJoin table("t1") select *
  )

  testQueryParsing(
    "SELECT * FROM t0 FULL JOIN t1",
    table('t0) outerJoin table("t1") select *
  )

  testQueryParsing(
    "SELECT * FROM t0 FULL OUTER JOIN t1",
    table('t0) outerJoin table("t1") select *
  )

  testQueryParsing(
    "SELECT * FROM t0 INNER JOIN t1 ON t0.a = t1.a",
    table('t0) join table("t1") on $"t0.a" === $"t1.a" select *
  )

  test("DISTINCT can't be used with star") {
    intercept[ParsingException] {
      parse("SELECT COUNT(DISTINCT *) FROM t0")
    }
  }

  testQueryParsing(
    "SELECT * FROM t0 a JOIN t1 b",
    table('t0) subquery 'a join (table("t1") subquery 'b) select *
  )

  testQueryParsing(
    "SELECT a.* FROM t0 a JOIN t1 b",
    table('t0) subquery 'a join (table("t1") subquery 'b) select $"a.*"
  )

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

  private def testQueryParsing(sql: String, expectedPlan: LogicalPlan): Unit = {
    test(s"parsing SQL: $sql") {
      checkPlan(parse(sql.split("\n").map(_.trim).mkString(" ")), expectedPlan)
    }
  }

  private def parse(query: String): LogicalPlan = new Parser parse query
}
