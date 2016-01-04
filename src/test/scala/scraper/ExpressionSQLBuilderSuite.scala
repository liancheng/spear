package scraper

import scraper.expressions.Literal
import scraper.expressions.Literal.{False, True}
import scraper.expressions.dsl._

class ExpressionSQLBuilderSuite extends SQLBuilderTest {
  test("literals") {
    checkSQL(True, "true")
    checkSQL(False, "false")
    checkSQL(Literal("foo"), "\"foo\"")
    checkSQL(Literal("\"foo"), "\"\\\"foo\"")
    checkSQL(Literal(0), "0")
    checkSQL(Literal(1: Byte), "CAST(1 AS TINYINT)")
    checkSQL(Literal(2: Short), "CAST(2 AS SMALLINT)")
    checkSQL(Literal(4: Long), "CAST(4 AS BIGINT)")
    checkSQL(Literal(1.1F), "CAST(1.1 AS FLOAT)")
    checkSQL(Literal(1.2D), "CAST(1.2 AS DOUBLE)")
  }

  test("arithmetic expressions") {
    checkSQL('a + 'b, "(`a` + `b`)")
    checkSQL('a - 'b, "(`a` - `b`)")
    checkSQL('a * 'b, "(`a` * `b`)")
    checkSQL('a / 'b, "(`a` / `b`)")
    checkSQL(-'a, "(-`a`)")
    checkSQL(+'a, "(+`a`)")
  }
}
