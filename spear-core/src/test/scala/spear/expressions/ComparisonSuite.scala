package spear.expressions

import org.scalatest.prop.Checkers

import spear.{LoggingFunSuite, TestUtils}
import spear.types.{BooleanType, IntType}

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
    checkStrictlyTyped('a.int === 'b.int, BooleanType)
    checkStrictlyTyped(('a.long cast IntType) === 'b.int, BooleanType)

    checkWellTyped('a.int === 'b.long, BooleanType)
    checkWellTyped('a.string === 'b.long, BooleanType)
  }

  test("IN") {
    checkWellTyped("1" in (1, 2, 3), BooleanType)
    checkWellTyped("1.0" in (1.0, 2, 3), BooleanType)
  }
}
