package scraper.expressions

import scala.util.Success

import scraper.LoggingFunSuite
import scraper.expressions.Cast.{booleanFalseStrings, booleanTrueStrings, widestTypeOf}
import scraper.types._

class CastSuite extends LoggingFunSuite {
  private val numericTypes = Seq(ByteType, ShortType, IntType, LongType, FloatType, DoubleType)

  private val primitiveTypes = numericTypes ++ Seq(BooleanType, StringType)

  test("implicit casts between numeric types") {
    for {
      (fromType, index) <- numericTypes.zipWithIndex
      toType <- numericTypes drop index
    } assert(fromType isCompatibleWith toType)
  }

  test("explicit casts between numeric types") {
    for {
      fromType <- numericTypes
      toType <- numericTypes
    } assert(fromType isCastableTo toType)
  }

  test("casts between primitive types and string type") {
    for (primitiveType <- primitiveTypes) {
      assert(primitiveType isCompatibleWith StringType)
      assert(primitiveType isCastableTo StringType)
      assert(StringType isCastableTo primitiveType)
    }

    assert(StringType isCompatibleWith BooleanType)
  }

  test("casts from null type to primitive types") {
    for (primitiveType <- primitiveTypes) {
      assert(NullType isCompatibleWith primitiveType)
      assert(NullType isCastableTo primitiveType)
    }
  }

  test("cast string type to boolean type") {
    for (trueString <- booleanTrueStrings) {
      assertResult(true) {
        (trueString cast BooleanType).evaluated
      }
    }

    for (falseString <- booleanFalseStrings) {
      assertResult(false) {
        (falseString cast BooleanType).evaluated
      }
    }

    intercept[Exception] {
      ("random" cast BooleanType).evaluated
    }
  }

  test("widest type") {
    assert(widestTypeOf(Seq(ByteType, IntType, ShortType)) == Success(IntType))
    assert(widestTypeOf(Seq(ByteType, ArrayType(IntType), IntType)).isFailure)
  }
}
