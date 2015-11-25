package scraper.expressions

import org.scalacheck.Prop.forAll
import org.scalatest.prop.Checkers
import scraper.LoggingFunSuite
import scraper.generators.values._
import scraper.generators.types._
import scraper.types.{IntegralType, NumericType, TestUtils}

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
      case (t: NumericType, a: Any, b: Any) =>
        val numeric = t.numeric.asInstanceOf[Numeric[Any]]
        Add(Literal(a, t), Literal(b, t)).evaluated == numeric.plus(a, b)
    })
  }

  test("minus") {
    check(forAll(genNumericPair) {
      case (t: NumericType, a: Any, b: Any) =>
        val numeric = t.numeric.asInstanceOf[Numeric[Any]]
        Minus(Literal(a, t), Literal(b, t)).evaluated == numeric.minus(a, b)
    })
  }

  test("multiply") {
    check(forAll(genNumericPair) {
      case (t: NumericType, a: Any, b: Any) =>
        val numeric = t.numeric.asInstanceOf[Numeric[Any]]
        Multiply(Literal(a, t), Literal(b, t)).evaluated == numeric.times(a, b)
    })
  }

  test("divide") {
    check(forAll(genIntegralPair) {
      case (t: IntegralType, a: Any, b: Any) if b != 0 =>
        val integral = t.integral.asInstanceOf[Integral[Any]]
        Divide(Literal(a, t), Literal(b, t)).evaluated == integral.quot(a, b)

      case (t: IntegralType, a: Any, b: Any) =>
        Divide(Literal(a, t), Literal(b, t)).evaluated == null
    })
  }
}
