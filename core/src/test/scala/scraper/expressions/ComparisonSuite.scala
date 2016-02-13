package scraper.expressions

import org.scalatest.prop.Checkers

import scraper.expressions.dsl._
import scraper.types.{BooleanType, IntType}
import scraper.{LoggingFunSuite, TestUtils}

class ComparisonSuite extends LoggingFunSuite with TestUtils with Checkers {
  test("a = b") {
    check { (a: Int, b: Int) =>
      Eq(Literal(a), Literal(b)).evaluated == (a == b)
    }
  }

  test("a > b") {
    check { (a: Int, b: Int) =>
      Gt(Literal(a), Literal(b)).evaluated == (a > b)
    }
  }

  test("a < b") {
    check { (a: Int, b: Int) =>
      Lt(Literal(a), Literal(b)).evaluated == (a < b)
    }
  }

  test("a >= b") {
    check { (a: Int, b: Int) =>
      GtEq(Literal(a), Literal(b)).evaluated == (a >= b)
    }
  }

  test("a <= b") {
    check { (a: Int, b: Int) =>
      LtEq(Literal(a), Literal(b)).evaluated == (a <= b)
    }
  }

  test("binary comparison type check") {
    checkStrictlyTyped('a.int =:= 'b.int, BooleanType)
    checkStrictlyTyped(('a.long cast IntType) =:= 'b.int, BooleanType)

    checkWellTyped('a.int =:= 'b.long, BooleanType)
    checkWellTyped('a.string =:= 'b.long, BooleanType)
  }
}
