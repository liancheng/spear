package scraper.parser

import scraper.{LoggingFunSuite, Test, TestUtils}
import scraper.exceptions.ParsingException
import scraper.expressions.Expression
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.plans.logical.LogicalPlan
import scraper.plans.logical.dsl._
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

  testExpressionParsing((Int.MaxValue.toLong + 1).toString, Int.MaxValue.toLong + 1)

  testExpressionParsing("\"1\"", "1")

  testExpressionParsing("'1'", "1")

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

  testExpressionParsing("a IS NOT NULL", 'a.notNull)

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

  test("invalid query") {
    intercept[ParsingException] {
      parse("garbage")
    }
  }

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
    table('t0) select '*
  )

  testQueryParsing(
    "SELECT a.* FROM t0 a",
    table('t0) subquery 'a select $"a.*"
  )

  testQueryParsing(
    "SELECT a FROM t0 WHERE a > 10",
    table('t0) where 'a > 10 select 'a
  )

  testQueryParsing(
    "SELECT * FROM t0 ORDER BY a",
    table('t0) select '* orderBy 'a.asc
  )

  testQueryParsing(
    "SELECT * FROM t0 ORDER BY a ASC",
    table('t0) select '* orderBy 'a.asc
  )

  testQueryParsing(
    "SELECT * FROM t0 ORDER BY a ASC NULLS FIRST",
    table('t0) select '* orderBy 'a.asc.nullsFirst
  )

  testQueryParsing(
    "SELECT * FROM t0 ORDER BY a ASC NULLS LAST",
    table('t0) select '* orderBy 'a.asc.nullsLast
  )

  testQueryParsing(
    "SELECT * FROM t0 ORDER BY a DESC",
    table('t0) select '* orderBy 'a.desc
  )

  testQueryParsing(
    "SELECT * FROM t0 ORDER BY a DESC NULLS FIRST",
    table('t0) select '* orderBy 'a.desc.nullsFirst
  )

  testQueryParsing(
    "SELECT * FROM t0 ORDER BY a DESC NULLS LAST",
    table('t0) select '* orderBy 'a.desc.nullsLast
  )

  testQueryParsing(
    "SELECT DISTINCT a FROM t0 WHERE a > 10",
    (table('t0) where 'a > 10 select 'a).distinct
  )

  testQueryParsing(
    "SELECT * FROM t0 LIMIT 1",
    table('t0) select '* limit 1
  )

  testQueryParsing(
    "SELECT * FROM t0, t1",
    table('t0) join table("t1") select '*
  )

  testQueryParsing(
    "SELECT * FROM t0 JOIN t1",
    table('t0) join table("t1") select '*
  )

  testQueryParsing(
    "SELECT * FROM t0 INNER JOIN t1",
    table('t0) join table("t1") select '*
  )

  testQueryParsing(
    "SELECT * FROM t0 LEFT SEMI JOIN t1",
    table('t0) leftSemiJoin table("t1") select '*
  )

  testQueryParsing(
    "SELECT * FROM t0 LEFT JOIN t1",
    table('t0) leftJoin table("t1") select '*
  )

  testQueryParsing(
    "SELECT * FROM t0 LEFT OUTER JOIN t1",
    table('t0) leftJoin table("t1") select '*
  )

  testQueryParsing(
    "SELECT * FROM t0 RIGHT OUTER JOIN t1",
    table('t0) rightJoin table("t1") select '*
  )

  testQueryParsing(
    "SELECT * FROM t0 FULL JOIN t1",
    table('t0) outerJoin table("t1") select '*
  )

  testQueryParsing(
    "SELECT * FROM t0 FULL OUTER JOIN t1",
    table('t0) outerJoin table("t1") select '*
  )

  testQueryParsing(
    "SELECT * FROM t0 INNER JOIN t1 ON t0.a = t1.a",
    table('t0) join table("t1") on $"t0.a" === $"t1.a" select '*
  )

  testQueryParsing(
    "SELECT 1 AS a UNION ALL SELECT 2 AS a",
    values(1 as 'a) union values(2 as 'a)
  )

  testQueryParsing(
    "SELECT * FROM t0 INTERSECT SELECT * FROM t1",
    table('t0) select '* intersect (table('t1) select '*)
  )

  testQueryParsing(
    "SELECT * FROM t0 EXCEPT SELECT * FROM t1",
    table('t0) select '* except (table('t1) select '*)
  )

  testQueryParsing(
    "SELECT COUNT(a) FROM t0",
    table('t0) select function('COUNT, 'a)
  )

  testQueryParsing(
    "SELECT COUNT(a) FROM t0 GROUP BY b",
    table('t0) groupBy 'b agg function('COUNT, 'a)
  )

  testQueryParsing(
    "SELECT COUNT(a) FROM t0 GROUP BY b HAVING COUNT(b) > 0",
    table('t0) groupBy 'b agg function('COUNT, 'a) having function('COUNT, 'b) > 0
  )

  testQueryParsing(
    "SELECT COUNT(a) FROM t0 GROUP BY b ORDER BY COUNT(b) ASC NULLS FIRST",
    table('t0) groupBy 'b agg function('COUNT, 'a) orderBy function('COUNT, 'b).asc.nullsFirst
  )

  testQueryParsing(
    "SELECT COUNT(DISTINCT a) FROM t0",
    table('t0) select distinctFunction('COUNT, 'a)
  )

  test("DISTINCT can't be used with star") {
    intercept[ParsingException] {
      parse("SELECT COUNT(DISTINCT *) FROM t0")
    }
  }

  testQueryParsing(
    "SELECT * FROM t0 a JOIN t1 b",
    table('t0) subquery 'a join (table("t1") subquery 'b) select '*
  )

  testQueryParsing(
    "SELECT a.* FROM t0 a JOIN t1 b",
    table('t0) subquery 'a join (table("t1") subquery 'b) select $"a.*"
  )

  testQueryParsing(
    "SELECT t.a FROM (SELECT * FROM t0) t",
    table('t0) select '* subquery 't select $"t.a"
  )

  testQueryParsing(
    "WITH c0 AS (SELECT 1) SELECT * FROM c0",
    let('c0 -> values(1)) {
      table('c0) select '*
    }
  )

  testQueryParsing(
    "WITH c0 AS (SELECT 1), c1 AS (SELECT 2) SELECT * FROM c0 UNION ALL SELECT * FROM c1",
    let('c1 -> values(2)) {
      let('c0 -> values(1)) {
        table('c0) select '* union (table('c1) select '*)
      }
    }
  )

  testQueryParsing(
    "SELECT 1 // comment",
    values(1)
  )

  testQueryParsing(
    "SELECT 1 -- comment",
    values(1)
  )

  testQueryParsing(
    "SELECT 1 # comment",
    values(1)
  )

  testQueryParsing(
    "SELECT 1 /* comment */",
    values(1)
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

  private def parse(query: String): LogicalPlan = new Parser(Test.defaultSettings) parse query
}
