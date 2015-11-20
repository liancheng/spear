package scraper.expressions

import org.scalacheck.Prop
import org.scalatest.prop.Checkers
import scraper.LoggingFunSuite
import scraper.types.{BooleanType, TestUtils}

class LogicalOperatorSuite extends LoggingFunSuite with TestUtils with Checkers {
  test("and") {
    check(Prop.forAll { (a: Boolean, b: Boolean) =>
      And(LogicalLiteral(a), LogicalLiteral(b)).evaluated == (a && b)
    })
  }

  test("or") {
    check(Prop.forAll { (a: Boolean, b: Boolean) =>
      Or(LogicalLiteral(a), LogicalLiteral(b)).evaluated == (a || b)
    })
  }

  test("not") {
    check(Prop.forAll { a: Boolean =>
      Not(LogicalLiteral(a)).evaluated == !a
    })
  }
}
