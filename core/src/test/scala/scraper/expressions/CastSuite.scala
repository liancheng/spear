package scraper.expressions

import scala.util.Success

import org.scalacheck.Prop.{all, forAll, BooleanOperators}
import org.scalatest.prop.Checkers
import scraper.LoggingFunSuite
import scraper.expressions.Cast.{booleanFalseStrings, booleanTrueStrings, compatible, widestTypeOf}
import scraper.types._

class CastSuite extends LoggingFunSuite with Checkers {
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

  test("array type") {
    assert(compatible(ArrayType(IntType), ArrayType(LongType)))
    assert(!compatible(ArrayType(LongType), ArrayType(IntType)))

    assert(compatible(ArrayType(IntType.!), ArrayType(LongType.!)))
    assert(compatible(ArrayType(IntType.!), ArrayType(LongType.?)))
    assert(compatible(ArrayType(IntType.?), ArrayType(LongType.?)))
    assert(!compatible(ArrayType(IntType.?), ArrayType(LongType.!)))

    assert(compatible(
      ArrayType(ArrayType(IntType)),
      ArrayType(ArrayType(LongType))
    ))
  }

  test("cast boolean values") {
    check {
      forAll { value: Boolean =>
        all(
          "cast from boolean to boolean" |:
            checkCast(value, BooleanType, value, BooleanType),

          "cast from boolean to int" |:
            checkCast(value, BooleanType, if (value) 1 else 0, IntType),

          "cast from boolean to string" |:
            checkCast(value, BooleanType, value.toString, StringType)
        )
      }
    }
  }

  test("cast byte values") {
    check {
      forAll { value: Byte =>
        all(
          "cast from byte to byte" |:
            checkCast(value, ByteType, value, ByteType),

          "cast from byte to short" |:
            checkCast(value, ByteType, value.toShort, ShortType),

          "cast from byte to int" |:
            checkCast(value, ByteType, value.toInt, IntType),

          "cast from byte to long" |:
            checkCast(value, ByteType, value.toLong, LongType),

          "cast from byte to float" |:
            checkCast(value, ByteType, value.toFloat, FloatType),

          "cast from byte to double" |:
            checkCast(value, ByteType, value.toDouble, DoubleType),

          "cast from byte to string" |:
            checkCast(value, ByteType, value.toString, StringType)
        )
      }
    }
  }

  test("cast short values") {
    check {
      forAll { value: Short =>
        all(
          "cast from short to byte" |:
            checkCast(value, ShortType, value.toByte, ByteType),

          "cast from short to short" |:
            checkCast(value, ShortType, value, ShortType),

          "cast from short to int" |:
            checkCast(value, ShortType, value.toInt, IntType),

          "cast from short to long" |:
            checkCast(value, ShortType, value.toLong, LongType),

          "cast from short to float" |:
            checkCast(value, ShortType, value.toFloat, FloatType),

          "cast from short to double" |:
            checkCast(value, ShortType, value.toDouble, DoubleType),

          "cast from short to string" |:
            checkCast(value, ShortType, value.toString, StringType)
        )
      }
    }
  }

  test("cast int values") {
    check {
      forAll { value: Int =>
        all(
          "cast from int to boolean" |:
            checkCast(value, IntType, value != 0, BooleanType),

          "cast from int to byte" |:
            checkCast(value, IntType, value.toByte, ByteType),

          "cast from int to short" |:
            checkCast(value, IntType, value.toShort, ShortType),

          "cast from int to int" |:
            checkCast(value, IntType, value, IntType),

          "cast from int to long" |:
            checkCast(value, IntType, value.toLong, LongType),

          "cast from int to float" |:
            checkCast(value, IntType, value.toFloat, FloatType),

          "cast from int to double" |:
            checkCast(value, IntType, value.toDouble, DoubleType),

          "cast from int to string" |:
            checkCast(value, IntType, value.toString, StringType)
        )
      }
    }
  }

  test("cast long values") {
    check {
      forAll { value: Long =>
        all(
          "cast from long to byte" |:
            checkCast(value, LongType, value.toByte, ByteType),

          "cast from long to short" |:
            checkCast(value, LongType, value.toShort, ShortType),

          "cast from long to int" |:
            checkCast(value, LongType, value.toInt, IntType),

          "cast from long to long" |:
            checkCast(value, LongType, value, LongType),

          "cast from long to float" |:
            checkCast(value, LongType, value.toFloat, FloatType),

          "cast from long to double" |:
            checkCast(value, LongType, value.toDouble, DoubleType),

          "cast from long to string" |:
            checkCast(value, LongType, value.toString, StringType)
        )
      }
    }
  }

  test("cast float values") {
    check {
      forAll { value: Float =>
        all(
          "cast from float to byte" |:
            checkCast(value, FloatType, value.toByte, ByteType),

          "cast from float to short" |:
            checkCast(value, FloatType, value.toShort, ShortType),

          "cast from float to int" |:
            checkCast(value, FloatType, value.toInt, IntType),

          "cast from float to long" |:
            checkCast(value, FloatType, value.toLong, LongType),

          "cast from float to float" |:
            checkCast(value, FloatType, value, FloatType),

          "cast from float to double" |:
            checkCast(value, FloatType, value.toDouble, DoubleType),

          "cast from float to string" |:
            checkCast(value, FloatType, value.toString, StringType)
        )
      }
    }
  }

  test("cast double values") {
    check {
      forAll { value: Double =>
        all(
          "cast from double to byte" |:
            checkCast(value, DoubleType, value.toByte, ByteType),

          "cast from double to short" |:
            checkCast(value, DoubleType, value.toShort, ShortType),

          "cast from double to int" |:
            checkCast(value, DoubleType, value.toInt, IntType),

          "cast from double to long" |:
            checkCast(value, DoubleType, value.toLong, LongType),

          "cast from double to double" |:
            checkCast(value, DoubleType, value.toFloat, FloatType),

          "cast from double to double" |:
            checkCast(value, DoubleType, value, DoubleType),

          "cast from double to string" |:
            checkCast(value, DoubleType, value.toString, StringType)
        )
      }
    }
  }

  private val numericTypes = Seq(ByteType, ShortType, IntType, LongType, FloatType, DoubleType)

  private val primitiveTypes = numericTypes ++ Seq(BooleanType, StringType)

  private def checkCast(value: Any, fromType: DataType, casted: Any, toType: DataType): Boolean = {
    Literal(value, fromType).cast(toType).evaluated == casted
  }
}
