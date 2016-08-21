package scraper

import scraper.expressions._
import scraper.expressions.Literal.{False, True}
import scraper.types._

class ExpressionSQLBuilderSuite extends SQLBuilderTest {
  test("literals") {
    checkSQL(Literal(null), "NULL")
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
    val a = 'a.int
    val b = 'b.int

    checkSQL(a + b, "(a + b)")
    checkSQL(a - b, "(a - b)")
    checkSQL(a * b, "(a * b)")
    checkSQL(a / b, "(a / b)")
    checkSQL(-a, "(-a)")
  }

  test("logical operators") {
    val a = 'a.boolean
    val b = 'b.boolean
    val c = 'c.boolean

    checkSQL(a && b, "(a AND b)")
    checkSQL(a || b, "(a OR b)")
    checkSQL(!a, "(NOT a)")
    checkSQL(If(a, b, c), "if(a, b, c)")
  }

  test("non-SQL expressions") {
    intercept[UnsupportedOperationException] {
      ('_.int.! at 0).sql.get
    }
  }

  test("casting") {
    val a = 'a.int

    checkSQL(a cast BooleanType, "CAST(a AS BOOLEAN)")
    checkSQL(a cast ByteType, "CAST(a AS TINYINT)")
    checkSQL(a cast ShortType, "CAST(a AS SMALLINT)")
    checkSQL(a cast IntType, "CAST(a AS INT)")
    checkSQL(a cast LongType, "CAST(a AS BIGINT)")
    checkSQL(a cast FloatType, "CAST(a AS FLOAT)")
    checkSQL(a cast DoubleType, "CAST(a AS DOUBLE)")
    checkSQL(a cast ArrayType(IntType.?), "CAST(a AS ARRAY<INT>)")
    checkSQL(a cast MapType(IntType, StringType.?), "CAST(a AS MAP<INT, STRING>)")
    checkSQL(
      a cast StructType('name -> StringType.?, 'age -> IntType.?),
      "CAST(a AS STRUCT<name: STRING, age: INT>)"
    )
  }
}
