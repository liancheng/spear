package scraper

import scraper.expressions.Literal.{False, True}
import scraper.expressions.dsl._
import scraper.expressions.{If, Literal}

class ExpressionSQLBuilderSuite extends SQLBuilderTest {
  test("literals") {
    checkSQL(True, "TRUE")
    checkSQL(False, "FALSE")
    checkSQL(Literal("foo"), "\"foo\"")
    checkSQL(Literal("\"foo"), "\"\\\"foo\"")
    checkSQL(Literal(0), "0")
    checkSQL(Literal(1: Byte), "CAST(1 AS TINYINT)")
    checkSQL(Literal(2: Short), "CAST(2 AS SMALLINT)")
    checkSQL(Literal(4L), "CAST(4 AS BIGINT)")
    checkSQL(Literal(1.1F), "CAST(1.1 AS FLOAT)")
    checkSQL(Literal(1.2D), "CAST(1.2 AS DOUBLE)")
  }

  test("arithmetic expressions") {
    checkSQL('a + 'b, "(`a` + `b`)")
    checkSQL('a - 'b, "(`a` - `b`)")
    checkSQL('a * 'b, "(`a` * `b`)")
    checkSQL('a / 'b, "(`a` / `b`)")
    checkSQL(-'a, "(-`a`)")
  }

  test("logical operators") {
    checkSQL('a && 'b, "(`a` AND `b`)")
    checkSQL('a || 'b, "(`a` OR `b`)")
    checkSQL(!'a, "(NOT `a`)")
    checkSQL(If('a, 'b, 'c), "IF(`a`, `b`, `c`)")
  }

  test("non-SQL expressions") {
    intercept[UnsupportedOperationException] {
      ('_.int.! at 0).sql.get
    }
  }
}
