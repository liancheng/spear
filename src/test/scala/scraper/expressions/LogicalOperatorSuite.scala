package scraper.expressions

import org.scalacheck.Prop
import org.scalatest.prop.Checkers
import scraper.LoggingFunSuite
import scraper.types.TestUtils

class LogicalOperatorSuite extends LoggingFunSuite with TestUtils with Checkers {
  test("a AND b") {
    check(Prop.forAll { (a: Boolean, b: Boolean) =>
      And(Literal(a), Literal(b)).evaluated == (a && b)
    })
  }

  test("a OR b") {
    check(Prop.forAll { (a: Boolean, b: Boolean) =>
      Or(Literal(a), Literal(b)).evaluated == (a || b)
    })
  }

  test("NOT a") {
    check(Prop.forAll { a: Boolean =>
      Not(Literal(a)).evaluated == !a
    })
  }
}
