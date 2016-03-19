package scraper.expressions

import org.scalatest.prop.Checkers

import scraper.{LoggingFunSuite, TestUtils}

class LogicalOperatorSuite extends LoggingFunSuite with TestUtils with Checkers {
  test("a AND b") {
    assert(And(Literal(true), Literal(0L)).isWellTyped === false)
    assert(And(Literal(true), Literal(0)).isWellTyped === true)
    assert(And(Literal(true), Literal(false)).isStrictlyTyped === true)

    check { (a: Boolean, b: Boolean) =>
      And(Literal(a), Literal(b)).evaluated == (a && b)
    }
  }

  test("a OR b") {
    assert(Or(Literal(true), Literal(0L)).isWellTyped === false)
    assert(Or(Literal(true), Literal(0)).isWellTyped === true)
    assert(Or(Literal(true), Literal(false)).isStrictlyTyped === true)

    check { (a: Boolean, b: Boolean) =>
      Or(Literal(a), Literal(b)).evaluated == (a || b)
    }
  }

  test("NOT a") {
    assert(Not(Literal(0L)).isWellTyped === false)
    assert(Not(Literal(0)).isWellTyped === true)
    assert(Not(Literal(true)).isStrictlyTyped === true)

    check { a: Boolean =>
      Not(Literal(a)).evaluated == !a
    }
  }
}
