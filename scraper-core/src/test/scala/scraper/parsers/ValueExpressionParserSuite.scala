package scraper.parsers

import fastparse.core.Logger

import scraper.{LoggingFunSuite, TestUtils}
import scraper.expressions._
import scraper.expressions.functions._
import scraper.types.IntType

class ValueExpressionParserSuite extends LoggingFunSuite with TestUtils {
  import fastparse.all._

  testExpressionParsing("1", 1)

  testExpressionParsing(Int.MaxValue.toString, Int.MaxValue)

  testExpressionParsing(Int.MinValue.toString, Int.MinValue)

  testExpressionParsing((Int.MaxValue.toLong + 1).toString, Int.MaxValue.toLong + 1)

  testExpressionParsing((Int.MinValue.toLong - 1).toString, Int.MinValue.toLong - 1)

  testExpressionParsing("'1'", "1")

  testExpressionParsing("'a' || 'b' || 'c'", concat(concat("a", "b"), "c"))

  testExpressionParsing("('a' || 'b') || 'c'", concat(concat("a", "b"), "c"))

  testExpressionParsing("'a' || ('b' || 'c')", concat("a", concat("b", "c")))

  testExpressionParsing("'a' 'b'", "ab")

  testExpressionParsing("true", true)

  testExpressionParsing("false", false)

  testExpressionParsing("\"a\"", 'a)

  testExpressionParsing("\"a\"\"b\"", Symbol("a\"b"))

  testExpressionParsing("(a = 1)", 'a === 1)

  testExpressionParsing("a AND b", 'a && 'b)

  testExpressionParsing("a OR b", 'a || 'b)

  testExpressionParsing("NOT a", !'a)

  testExpressionParsing("(a AND a) AND a", ('a && 'a) && 'a)

  testExpressionParsing("a = b", 'a === 'b)

  testExpressionParsing("a <> b", 'a =/= 'b)

  testExpressionParsing("a > b", 'a > 'b)

  testExpressionParsing("a >= b", 'a >= 'b)

  testExpressionParsing("a < b", 'a < 'b)

  testExpressionParsing("a <= b", 'a <= 'b)

  testExpressionParsing("a IS NULL", 'a.isNull)

  testExpressionParsing("a IS NOT NULL", 'a.isNotNull)

  testExpressionParsing("-a", -'a)

  testExpressionParsing("a + b", 'a + 'b)

  testExpressionParsing("a - b", 'a - 'b)

  testExpressionParsing("a * b", 'a * 'b)

  testExpressionParsing("a / b", 'a / 'b)

  testExpressionParsing("a % b", 'a % 'b)

  testExpressionParsing("a ^ b", 'a ^ 'b)

  testExpressionParsing("a + b * c - d / e", 'a + ('b * 'c) - ('d / 'e))

  testExpressionParsing("a + b * (c - d) / e", 'a + 'b * ('c - 'd) / 'e)

  testExpressionParsing("a + b * c ^ d", 'a + ('b * ('c ^ 'd)))

  testExpressionParsing("(a + b) + c", ('a + 'b) + 'c)

  testExpressionParsing("a + (b + c)", 'a + ('b + 'c))

  testExpressionParsing("CAST(RAND(42) * 100 AS INT)", ('rand(42) * 100) cast IntType)

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
    "CASE a WHEN 1, 2 THEN 'x' WHEN 3, 4 THEN 'y' ELSE 'z' END",
    when('a === 1 || 'a === 2, "x") when ('a === 3 || 'a === 4, "y") otherwise "z"
  )

  testExpressionParsing(
    "IF(a > 0, 1, 2)",
    If('a > 0, 1, 2)
  )

  private def testExpressionParsing(input: String, expected: Expression): Unit = {
    test(s"parsing expression: $input") {
      checkTree(parse(input), expected)
    }
  }

  private def parse(input: String): Expression = {
    implicit val parserLogger = Logger(logInfo(_))
    (Start ~ ValueExpressionParser.valueExpression.log() ~ End parse input.trim).get.value
  }
}
