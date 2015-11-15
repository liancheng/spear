package scraper.expressions

import org.scalacheck.Prop.forAll
import org.scalatest.prop.Checkers
import scraper.LoggingFunSuite
import scraper.types.{ IntType, TestUtils }

class ArithmeticExpressionSuite extends LoggingFunSuite with TestUtils with Checkers {
  test("add") {
    check(forAll { (a: Int, b: Int) =>
      Add(Literal(a, IntType), Literal(b, IntType)).evaluated == a + b
    })
  }

  test("minus") {
    check(forAll { (a: Int, b: Int) =>
      Minus(Literal(a, IntType), Literal(b, IntType)).evaluated == a - b
    })
  }

  test("multiply") {
    check(forAll { (a: Int, b: Int) =>
      Multiply(Literal(a, IntType), Literal(b, IntType)).evaluated == a * b
    })
  }
}
