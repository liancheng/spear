package scraper.expressions

import org.scalacheck.Prop
import org.scalacheck.Prop.forAll
import org.scalatest.prop.Checkers
import scraper.LoggingFunSuite
import scraper.generators.types._
import scraper.generators.values._
import scraper.types.TestUtils

class ArithmeticExpressionSuite extends LoggingFunSuite with TestUtils with Checkers {
  val genNumericPair = for {
    t <- genNumericType
    a <- genValueForNumericType(t)
    b <- genValueForNumericType(t)
  } yield (t, a, b)

  val genIntegralPair = for {
    t <- genIntegralType
    a <- genValueForIntegralType(t)
    b <- genValueForIntegralType(t)
  } yield (t, a, b)

  val genFractionalPair = for {
    t <- genFractionalType
    a <- genValueForFractionalType(t)
    b <- genValueForFractionalType(t)
  } yield (t, a, b)

  test("add") {
    check(forAll(genNumericPair) {
      case (t, a, b) =>
        val numeric = t.numeric.asInstanceOf[Numeric[Any]]
        Add(Literal(a, t), Literal(b, t)).evaluated == numeric.plus(a, b)
    })
  }

  test("minus") {
    check(forAll(genNumericPair) {
      case (t, a, b) =>
        val numeric = t.numeric.asInstanceOf[Numeric[Any]]
        Minus(Literal(a, t), Literal(b, t)).evaluated == numeric.minus(a, b)
    })
  }

  test("multiply") {
    check(forAll(genNumericPair) {
      case (t, a, b) =>
        val numeric = t.numeric.asInstanceOf[Numeric[Any]]
        Multiply(Literal(a, t), Literal(b, t)).evaluated == numeric.times(a, b)
    })
  }

  test("divide") {
    check(forAll(genIntegralPair) {
      case (t, a, b) =>
        Prop.classify(b == 0, "divide by zero", "divide by non-zero") {
          if (b == 0) {
            Divide(Literal(a, t), Literal(b, t)).evaluated == null
          } else {
            val integral = t.integral.asInstanceOf[Integral[Any]]
            Divide(Literal(a, t), Literal(b, t)).evaluated == integral.quot(a, b)
          }
        }
    })
  }
}
