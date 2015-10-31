package scraper.expressions

import org.scalacheck.Prop
import org.scalatest.prop.Checkers
import scraper.types.{ BooleanType, TestUtils }

class LogicalOperatorSuite extends TestUtils with Checkers {
  test("and") {
    check(Prop.forAll { (a: Boolean, b: Boolean) =>
      And(Literal(a, BooleanType), Literal(b, BooleanType)).evaluated == (a && b)
    })
  }

  test("or") {
    check(Prop.forAll { (a: Boolean, b: Boolean) =>
      Or(Literal(a, BooleanType), Literal(b, BooleanType)).evaluated == (a || b)
    })
  }

  test("not") {
    check(Prop.forAll { a: Boolean =>
      Not(Literal(a, BooleanType)).evaluated == !a
    })
  }
}
