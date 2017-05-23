package spear.expressions

import org.scalatest.prop.Checkers

import spear.{LoggingFunSuite, TestUtils}

class LogicalOperatorSuite extends LoggingFunSuite with TestUtils with Checkers {
  test("a AND b") {
    assert(!And(Literal(true), Literal(0L)).isWellTyped)
    assert(And(Literal(true), Literal(0)).isWellTyped)
    assert(And(Literal(true), Literal(false)).isStrictlyTyped)

    check { (a: Boolean, b: Boolean) =>
      And(Literal(a), Literal(b)).evaluated == (a && b)
    }
  }

  test("a OR b") {
    assert(!Or(Literal(true), Literal(0L)).isWellTyped)
    assert(Or(Literal(true), Literal(0)).isWellTyped)
    assert(Or(Literal(true), Literal(false)).isStrictlyTyped)

    check { (a: Boolean, b: Boolean) =>
      Or(Literal(a), Literal(b)).evaluated == (a || b)
    }
  }

  test("NOT a") {
    assert(!Not(Literal(0L)).isWellTyped)
    assert(Not(Literal(0)).isWellTyped)
    assert(Not(Literal(true)).isStrictlyTyped)

    check { a: Boolean =>
      Not(Literal(a)).evaluated == !a
    }
  }
}
