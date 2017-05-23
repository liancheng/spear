package spear.expressions

import scala.util.Success

import org.scalacheck.Arbitrary._
import org.scalacheck.Prop
import org.scalacheck.Prop.{all, forAll, BooleanOperators}
import org.scalatest.prop.Checkers

import spear.LoggingFunSuite
import spear.exceptions.TypeCastException
import spear.expressions.Cast.{booleanFalseStrings, booleanTrueStrings, widestTypeOf}
import spear.expressions.functions.lit
import spear.types._

class CastSuite extends LoggingFunSuite with Checkers {
  test("invalid casting") {
    intercept[TypeCastException] {
      (lit(1) cast ArrayType(IntType)).strictlyTyped.get
    }
  }

  test("implicit casting between numeric types") {
    for {
      (fromType, index) <- numericTypes.zipWithIndex
      toType <- numericTypes drop index
    } assert(fromType isCastableTo toType)
  }

  test("explicit casting between numeric types") {
    for {
      fromType <- numericTypes
      toType <- numericTypes
    } assert(fromType isCastableTo toType)
  }

  test("casting between primitive types and string type") {
    for (primitiveType <- primitiveTypes) {
      assert(primitiveType isCastableTo StringType)
      assert(StringType isCastableTo primitiveType)
    }

    assert(StringType isCompatibleWith BooleanType)
  }

  test("casting from null type to primitive types") {
    for (primitiveType <- primitiveTypes) {
      assert(NullType isCompatibleWith primitiveType)
      assert(NullType isCastableTo primitiveType)
    }
  }

  test("casting string type to boolean type") {
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

  test("casting boolean values") {
    check {
      forAll { value: Boolean =>
        all(
          checkCast(value of BooleanType, value of BooleanType),
          checkCast(value of BooleanType, if (value) 1 else 0 of IntType),
          checkCast(value of BooleanType, value.toString of StringType)
        )
      }
    }
  }

  test("casting byte values") {
    check {
      forAll { value: Byte =>
        all(
          checkCast(value of ByteType, value of ByteType),
          checkCast(value of ByteType, value.toShort of ShortType),
          checkCast(value of ByteType, value.toInt of IntType),
          checkCast(value of ByteType, value.toLong of LongType),
          checkCast(value of ByteType, value.toFloat of FloatType),
          checkCast(value of ByteType, value.toDouble of DoubleType),
          checkCast(value of ByteType, value.toString of StringType)
        )
      }
    }
  }

  test("casting short values") {
    check {
      forAll { value: Short =>
        all(
          checkCast(value of ShortType, value.toByte of ByteType),
          checkCast(value of ShortType, value of ShortType),
          checkCast(value of ShortType, value.toInt of IntType),
          checkCast(value of ShortType, value.toLong of LongType),
          checkCast(value of ShortType, value.toFloat of FloatType),
          checkCast(value of ShortType, value.toDouble of DoubleType),
          checkCast(value of ShortType, value.toString of StringType)
        )
      }
    }
  }

  test("casting int values") {
    check {
      forAll { value: Int =>
        all(
          checkCast(value of IntType, value != 0 of BooleanType),
          checkCast(value of IntType, value.toByte of ByteType),
          checkCast(value of IntType, value.toShort of ShortType),
          checkCast(value of IntType, value of IntType),
          checkCast(value of IntType, value.toLong of LongType),
          checkCast(value of IntType, value.toFloat of FloatType),
          checkCast(value of IntType, value.toDouble of DoubleType),
          checkCast(value of IntType, value.toString of StringType)
        )
      }
    }
  }

  test("casting long values") {
    check {
      forAll { value: Long =>
        all(
          checkCast(value of LongType, value.toByte of ByteType),
          checkCast(value of LongType, value.toShort of ShortType),
          checkCast(value of LongType, value.toInt of IntType),
          checkCast(value of LongType, value of LongType),
          checkCast(value of LongType, value.toFloat of FloatType),
          checkCast(value of LongType, value.toDouble of DoubleType),
          checkCast(value of LongType, value.toString of StringType)
        )
      }
    }
  }

  test("casting float values") {
    check {
      forAll { value: Float =>
        all(
          checkCast(value of FloatType, value.toByte of ByteType),
          checkCast(value of FloatType, value.toShort of ShortType),
          checkCast(value of FloatType, value.toInt of IntType),
          checkCast(value of FloatType, value.toLong of LongType),
          checkCast(value of FloatType, value of FloatType),
          checkCast(value of FloatType, value.toDouble of DoubleType),
          checkCast(value of FloatType, value.toString of StringType)
        )
      }
    }
  }

  test("casting double values") {
    check {
      forAll { value: Double =>
        all(
          checkCast(value of DoubleType, value.toByte of ByteType),
          checkCast(value of DoubleType, value.toShort of ShortType),
          checkCast(value of DoubleType, value.toInt of IntType),
          checkCast(value of DoubleType, value.toLong of LongType),
          checkCast(value of DoubleType, value.toFloat of FloatType),
          checkCast(value of DoubleType, value of DoubleType),
          checkCast(value of DoubleType, value.toString of StringType)
        )
      }
    }
  }

  private val numericTypes = Seq(ByteType, ShortType, IntType, LongType, FloatType, DoubleType)

  private val primitiveTypes = numericTypes ++ Seq(BooleanType, StringType)

  private def checkCast(from: Literal, to: Literal): Prop = {
    val cast = from cast to.dataType
    cast.debugString |: (cast.evaluated == to.value)
  }
}
