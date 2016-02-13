package scraper.expressions

import scraper.expressions.dsl._
import scraper.types.{IntType, LongType}
import scraper.{LoggingFunSuite, TestUtils}

class NullExpressionsSuite extends LoggingFunSuite with TestUtils {
  test("if - type check") {
    checkStrictlyTyped(If(true, 'a.int, 'b.int), IntType)
    checkStrictlyTyped(If('a.int =:= 1, 'a.int, 'b.int), IntType)

    checkWellTyped(If(true, 'a.long, 'b.int), LongType)
    checkWellTyped(If('a.int =:= 1L, 'a.int, 'b.int), IntType)
    checkWellTyped(If('a.int =:= 1L, 'a.long, 'b.int), LongType)
  }
}
