package scraper.generators

import org.scalacheck.Arbitrary._
import org.scalacheck.{Arbitrary, Gen}
import scraper.Test._
import scraper.config.Settings
import scraper.config.Settings.Key
import scraper.types._

package object types {
  val AllowNullType: Key[Boolean] =
    Key("scraper.test.types.allow-null-type").boolean

  val AllowEmptyTupleType: Key[Boolean] =
    Key("scraper.test.types.allow-empty-tuple-type").boolean

  val AllowNullableComplexType: Key[Boolean] =
    Key("scraper.test.types.allow-nullable-complex-type").boolean

  val AllowNullableArrayType: Key[Boolean] =
    Key("scraper.test.types.allow-nullable-array-type").boolean

  val AllowNullableMapType: Key[Boolean] =
    Key("scraper.test.types.allow-nullable-map-type").boolean

  val AllowNullableTupleField: Key[Boolean] =
    Key("scraper.test.types.allow-nullable-tuple-field").boolean

  val AllowNestedTupleType: Key[Boolean] =
    Key("scraper.test.types.allow-nested-tuple-type").boolean

  def genDataType(implicit settings: Settings): Gen[DataType] = Gen sized {
    case 0 => Gen.fail
    case 1 => genPrimitiveType(settings)
    case _ => Gen oneOf (genPrimitiveType(settings), genComplexType(settings))
  }

  implicit val arbDataType: Arbitrary[DataType] = Arbitrary(genDataType(defaultSettings))

  def genPrimitiveType(implicit settings: Settings): Gen[PrimitiveType] =
    if (settings(AllowNullType)) {
      Gen oneOf (genNumericType, Gen.const(StringType), Gen.const(BooleanType))
    } else {
      Gen oneOf (
        genNumericType,
        Gen const StringType,
        Gen const BooleanType,
        Gen const NullType
      )
    }

  implicit val arbPrimitiveType: Arbitrary[PrimitiveType] =
    Arbitrary(genPrimitiveType(defaultSettings))

  def genNumericType: Gen[NumericType] = Gen oneOf (genIntegralType, genFractionalType)

  implicit val arbNumericType: Arbitrary[NumericType] = Arbitrary(genNumericType)

  def genIntegralType: Gen[IntegralType] = Gen oneOf (
    Gen const ByteType,
    Gen const ShortType,
    Gen const IntType,
    Gen const LongType
  )

  implicit val arbIntegralType: Arbitrary[IntegralType] = Arbitrary(genIntegralType)

  def genFractionalType: Gen[FractionalType] = Gen oneOf (
    Gen const FloatType,
    Gen const DoubleType
  )

  implicit val arbFractionalType: Arbitrary[FractionalType] = Arbitrary(genFractionalType)

  def genComplexType(implicit settings: Settings): Gen[ComplexType] = Gen oneOf (
    genArrayType(settings),
    genMapType(settings),
    genTupleType(settings)
  )

  implicit val arbComplexType: Arbitrary[ComplexType] = Arbitrary(genComplexType(defaultSettings))

  def genArrayType(implicit settings: Settings): Gen[ArrayType] = Gen sized {
    case size if size < 2 =>
      Gen.fail

    case size =>
      Gen resize (size - 1, for {
        elementType <- genDataType(settings)
        allowNullable = settings(AllowNullableArrayType)
        elementNullable <- if (allowNullable) arbitrary[Boolean] else Gen const false
      } yield ArrayType(elementType, elementNullable))
  }

  implicit val arbArrayType: Arbitrary[ArrayType] = Arbitrary(genArrayType(defaultSettings))

  def genMapType(implicit settings: Settings): Gen[MapType] = Gen sized {
    case size if size < 3 =>
      Gen.fail

    case size =>
      Gen resize (size - 2, for {
        keyType <- genPrimitiveType(settings)
        valueType <- genDataType(settings)

        allowNullable = settings(AllowNullableMapType)
        valueNullable <- if (allowNullable) arbitrary[Boolean] else Gen const false
      } yield MapType(keyType, valueType, valueNullable))
  }

  implicit val arbMapType: Arbitrary[MapType] = Arbitrary(genMapType(defaultSettings))

  def genTupleType(implicit settings: Settings): Gen[TupleType] = Gen sized {
    case 0 =>
      Gen.fail

    case size =>
      Gen resize (size - 1, for {
        fieldsSize <- Gen.size

        fieldNumUpperBound = fieldsSize / 2
        fieldNumLowerBound = if (settings(AllowEmptyTupleType)) 0 else 1

        fieldNum <- Gen choose (fieldNumLowerBound, fieldNumUpperBound)
        fieldTypeUpperBound = fieldNumUpperBound / (fieldNum max 1)

        genFieldType = Gen resize (fieldTypeUpperBound, if (settings(AllowNestedTupleType)) {
          genDataType(settings)
        } else {
          genPrimitiveType(settings)
        })

        allowNullable = settings(AllowNullableTupleField)
        genNullable = if (allowNullable) arbitrary[Boolean] else Gen const false

        genFieldSpec = for {
          fieldType <- genFieldType
          nullable <- genNullable
        } yield FieldSpec(fieldType, nullable)

        fieldSpecs <- Gen listOfN (fieldNum, genFieldSpec)

        fields = fieldSpecs.zipWithIndex map {
          case (fieldSpec, ordinal) => TupleField(s"c$ordinal", fieldSpec)
        }
      } yield TupleType(fields))
  }

  implicit val arbTupleType: Arbitrary[TupleType] = Arbitrary(genTupleType(defaultSettings))
}
