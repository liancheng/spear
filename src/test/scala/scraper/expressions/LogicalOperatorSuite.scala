package scraper.expressions

import org.scalacheck.Prop
import org.scalatest.prop.Checkers
import scraper.LoggingFunSuite
import scraper.types.TestUtils

class LogicalOperatorSuite extends LoggingFunSuite with TestUtils with Checkers {
  test("a AND b") {
    check(Prop.forAll { (a: Boolean, b: Boolean) =>
      And(LogicalLiteral(a), LogicalLiteral(b)).evaluated == (a && b)
    })
  }

  test("a OR b") {
    check(Prop.forAll { (a: Boolean, b: Boolean) =>
      Or(LogicalLiteral(a), LogicalLiteral(b)).evaluated == (a || b)
    })
  }

  test("NOT a") {
    check(Prop.forAll { a: Boolean =>
      Not(LogicalLiteral(a)).evaluated == !a
    })
  }
}
