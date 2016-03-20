package scraper.expressions

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.scalatest.prop.Checkers

import scraper.{LoggingFunSuite, TestUtils}
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.generators.types._
import scraper.generators.values._
import scraper.types._

class ArithmeticExpressionSuite extends LoggingFunSuite with TestUtils with Checkers {
  private val genNumericLiteral: Gen[Literal] = for {
    t <- genNumericType
    v <- genValueForNumericType(t)
  } yield Literal(v, t)

  private val genNumericLiteralPair = for {
    t <- genNumericType
    a <- genValueForNumericType(t)
    b <- genValueForNumericType(t)
  } yield (Literal(a, t), Literal(b, t))

  test("plus") {
    check(forAll(genNumericLiteralPair) {
      case (a @ Literal(_, t: NumericType), b) =>
        Plus(a, b).evaluated == t.genericNumeric.plus(a.value, b.value)
    })
  }

  test("minus") {
    check(forAll(genNumericLiteralPair) {
      case (a @ Literal(_, t: NumericType), b) =>
        Minus(a, b).evaluated == t.genericNumeric.minus(a.value, b.value)
    })
  }

  test("multiply") {
    check(forAll(genNumericLiteralPair) {
      case (a @ Literal(_, t: NumericType), b) =>
        Multiply(a, b).evaluated == t.genericNumeric.times(a.value, b.value)
    })
  }

  test("divide") {
    check(forAll(genNumericLiteralPair) {
      case (a @ Literal(_, t: IntegralType), b) =>
        if (b.value == 0) {
          Divide(a, b).evaluated == null
        } else {
          Divide(a, b).evaluated == t.genericIntegral.quot(a.value, b.value)
        }

      case (a @ Literal(_, t: FractionalType), b) =>
        if (b.value == 0D) {
          Divide(a, b).evaluated == null
        } else {
          Divide(a, b).evaluated == t.genericFractional.div(a.value, b.value)
        }
    })
  }

  test("negate") {
    check(forAll(genNumericLiteral) {
      case lit @ Literal(v, t: NumericType) =>
        val numeric = t.numeric.asInstanceOf[Numeric[Any]]
        Negate(lit).evaluated == numeric.negate(v)
    })
  }

  test("binary arithmetic expression type check") {
    checkStrictlyTyped(lit(1) + 1, IntType)
    checkStrictlyTyped(lit(1L) + 1L, LongType)

    checkWellTyped(lit(1) + "1", DoubleType)
    checkWellTyped(lit(1D) + "1", DoubleType)
  }
}
