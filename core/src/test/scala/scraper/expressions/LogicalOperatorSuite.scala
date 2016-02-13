package scraper.expressions

import org.scalatest.prop.Checkers

import scraper.{TestUtils, LoggingFunSuite}

class LogicalOperatorSuite extends LoggingFunSuite with TestUtils with Checkers {
  test("a AND b") {
    assert(And(Literal(true), Literal(0L)).wellTyped === false)
    assert(And(Literal(true), Literal(0)).wellTyped === true)
    assert(And(Literal(true), Literal(false)).strictlyTyped === true)

    check { (a: Boolean, b: Boolean) =>
      And(Literal(a), Literal(b)).evaluated == (a && b)
    }
  }

  test("a OR b") {
    assert(Or(Literal(true), Literal(0L)).wellTyped === false)
    assert(Or(Literal(true), Literal(0)).wellTyped === true)
    assert(Or(Literal(true), Literal(false)).strictlyTyped === true)

    check { (a: Boolean, b: Boolean) =>
      Or(Literal(a), Literal(b)).evaluated == (a || b)
    }
  }

  test("NOT a") {
    assert(Not(Literal(0L)).wellTyped === false)
    assert(Not(Literal(0)).wellTyped === true)
    assert(Not(Literal(true)).strictlyTyped === true)

    check { a: Boolean =>
      Not(Literal(a)).evaluated == !a
    }
  }
}
