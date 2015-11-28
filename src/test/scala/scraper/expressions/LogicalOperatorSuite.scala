package scraper.expressions

import org.scalatest.prop.Checkers
import scraper.LoggingFunSuite
import scraper.types.TestUtils

class LogicalOperatorSuite extends LoggingFunSuite with TestUtils with Checkers {
  test("a AND b") {
    check { (a: Boolean, b: Boolean) =>
      And(Literal(a), Literal(b)).evaluated == (a && b)
    }
  }

  test("a OR b") {
    check { (a: Boolean, b: Boolean) =>
      Or(Literal(a), Literal(b)).evaluated == (a || b)
    }
  }

  test("NOT a") {
    check { a: Boolean =>
      Not(Literal(a)).evaluated == !a
    }
  }
}
