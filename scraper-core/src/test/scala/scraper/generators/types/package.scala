package scraper.generators

import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Arbitrary._

import scraper.Name.caseInsensitive
import scraper.Test._
import scraper.config.Settings
import scraper.generators.Keys._
import scraper.types._

package object types {
  def genSubtypeOf(supertype: AbstractDataType)(
    implicit
    settings: Settings
  ): Gen[DataType] = supertype match {
    case IntegralType   => genIntegralType
    case FractionalType => genFractionalType
    case NumericType    => genNumericType
    case PrimitiveType  => genPrimitiveType(settings)
    case MapType        => genMapType(settings)
    case ArrayType      => genArrayType(settings)
    case StructType     => genStructType(settings)
  }

  def genDataType(implicit settings: Settings): Gen[DataType] = Gen.sized {
    case upperBound if upperBound < 2 =>
      genPrimitiveType(settings)

    case _ =>
      Gen.oneOf(
        genPrimitiveType(settings),
        genComplexType(settings)
      )
  }

  implicit val arbDataType: Arbitrary[DataType] = Arbitrary(genDataType(defaultSettings))

  def genPrimitiveType(implicit settings: Settings): Gen[PrimitiveType] =
    genSubtypeOf(PrimitiveType, {
      if (settings(AllowNullType)) {
        Gen.oneOf(
          genNumericType,
          Gen.const(StringType),
          Gen.const(BooleanType),
          Gen.const(NullType)
        )
      } else {
        Gen.oneOf(
          genNumericType,
          Gen.const(StringType),
          Gen.const(BooleanType)
        )
      }
    })

  implicit val arbPrimitiveType: Arbitrary[PrimitiveType] =
    Arbitrary(genPrimitiveType(defaultSettings))

  def genNumericType: Gen[NumericType] = Gen.oneOf(genIntegralType, genFractionalType)

  implicit val arbNumericType: Arbitrary[NumericType] = Arbitrary(genNumericType)

  def genIntegralType: Gen[IntegralType] = Gen.oneOf(
    Gen.const(ByteType),
    Gen.const(ShortType),
    Gen.const(IntType),
    Gen.const(LongType)
  )

  implicit val arbIntegralType: Arbitrary[IntegralType] = Arbitrary(genIntegralType)

  def genFractionalType: Gen[FractionalType] = Gen.oneOf(
    Gen.const(FloatType),
    Gen.const(DoubleType)
  )

  implicit val arbFractionalType: Arbitrary[FractionalType] = Arbitrary(genFractionalType)

  def genComplexType(implicit settings: Settings): Gen[ComplexType] = Gen.oneOf(
    genArrayType(settings),
    genMapType(settings),
    genStructType(settings)
  )

  implicit val arbComplexType: Arbitrary[ComplexType] = Arbitrary(genComplexType(defaultSettings))

  def genArrayType(implicit settings: Settings): Gen[ArrayType] = genSubtypeOf(ArrayType, {
    for {
      elementType <- genDataType(settings)
      allowNullable = settings(AllowNullableArrayType)
      elementNullable <- if (allowNullable) arbitrary[Boolean] else Gen.const(false)
    } yield ArrayType(elementType, elementNullable)
  })

  implicit val arbArrayType: Arbitrary[ArrayType] = Arbitrary(genArrayType(defaultSettings))

  def genMapType(implicit settings: Settings): Gen[MapType] = genSubtypeOf(MapType, {
    for {
      keyType <- genPrimitiveType(settings)
      valueType <- genDataType(settings)

      allowNullable = settings(AllowNullableMapType)
      valueNullable <- if (allowNullable) arbitrary[Boolean] else Gen.const(false)
    } yield MapType(keyType, valueType, valueNullable)
  })

  implicit val arbMapType: Arbitrary[MapType] = Arbitrary(genMapType(defaultSettings))

  def genStructType(implicit settings: Settings): Gen[StructType] = genSubtypeOf(StructType, {
    for {
      fieldsUpperBound <- Gen.size

      maxFieldNum = fieldsUpperBound min settings(MaxStructTypeWidth)
      minFieldNum = if (settings(AllowEmptyStructType)) 0 else 1

      fieldNum <- Gen.choose(minFieldNum, maxFieldNum)
      fieldTypeUpperBound = fieldsUpperBound / (fieldNum max 1)

      genFieldType = Gen.resize(
        fieldTypeUpperBound,
        if (settings(AllowNestedStructType)) genDataType(settings) else genPrimitiveType(settings)
      )

      genFieldSpec = for {
        fieldType <- genFieldType
        nullable <- if (settings(AllowNullableStructField)) arbitrary[Boolean] else Gen.const(false)
      } yield FieldSpec(fieldType, nullable)

      fieldSpecs <- Gen.listOfN(fieldNum, genFieldSpec)

      fields = fieldSpecs.zipWithIndex map {
        case (fieldSpec, ordinal) =>
          StructField(caseInsensitive(s"c$ordinal"), fieldSpec)
      }
    } yield StructType(fields)
  })

  implicit val arbStructType: Arbitrary[StructType] = Arbitrary(genStructType(defaultSettings))

  private def genSubtypeOf[T <: DataType](supertype: AbstractDataType, gen: => Gen[T]): Gen[T] = {
    def lowerBoundOf(supertype: AbstractDataType): Int = supertype match {
      case PrimitiveType => 1
      case ArrayType     => 2
      case MapType       => 3
      case StructType    => 1
    }

    genBoundedTree(lowerBoundOf(supertype), gen)
  }

  private def genBoundedTree[T](lowerBound: Int, gen: => Gen[T]): Gen[T] =
    Gen.sized {
      case upperBound if upperBound < lowerBound => Gen.fail
      case upperBound                            => Gen.resize(upperBound - 1, gen)
    }
}
