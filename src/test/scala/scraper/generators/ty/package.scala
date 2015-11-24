package scraper.generators

import org.scalacheck.Arbitrary._
import org.scalacheck.{Gen, Test}
import scraper.types._

package object ty {
  trait DataTypeParameters extends Test.Parameters.Default {
    def allowNullType: Boolean = true

    def allowNullableComplexType: Boolean = true

    def allowNullableArrayType: Boolean = allowNullableComplexType

    def allowNullableMapType: Boolean = allowNullableComplexType

    def allowNullableTupleField: Boolean = allowNullableComplexType

    def allowEmptyTupleType: Boolean = true
  }

  object DataTypeParameters {
    val default = new DataTypeParameters {}
  }

  def genDataType: Gen[DataType] = Gen.sized {
    case 0 => Gen.fail
    case 1 => genPrimitiveType
    case _ => Gen.oneOf(genPrimitiveType, genComplexType)
  }

  def genPrimitiveType: Gen[PrimitiveType] = Gen.parameterized {
    case params: DataTypeParameters =>
      if (params.allowNullType) {
        Gen.oneOf(genNumericType, Gen.const(StringType), Gen.const(BooleanType))
      } else {
        Gen.oneOf(
          genNumericType, Gen.const(StringType), Gen.const(BooleanType), Gen.const(NullType)
        )
      }
  }

  def genNumericType: Gen[NumericType] = Gen.oneOf(genIntegralType, genFractionalType)

  def genIntegralType: Gen[IntegralType] = Gen.oneOf(
    Gen.const(ByteType), Gen.const(ShortType), Gen.const(IntType), Gen.const(LongType)
  )

  def genFractionalType: Gen[FractionalType] = Gen.oneOf(
    Gen.const(FloatType), Gen.const(DoubleType)
  )

  def genComplexType: Gen[ComplexType] = Gen.oneOf(genArrayType, genMapType, genTupleType)

  def genArrayType: Gen[ArrayType] = Gen.parameterized {
    case params: DataTypeParameters =>
      Gen.sized {
        case size if size < 2 =>
          Gen.fail

        case size =>
          Gen.resize(size - 1, for {
            elementType <- genDataType

            allowNullable = params.allowNullableArrayType
            elementNullable <- if (allowNullable) arbitrary[Boolean] else Gen.const(false)
          } yield ArrayType(elementType, elementNullable))
      }
  }

  def genMapType: Gen[MapType] = Gen.parameterized {
    case params: DataTypeParameters =>
      Gen.sized {
        case size if size < 3 =>
          Gen.fail

        case size =>
          Gen.resize(size - 2, for {
            keyType <- genPrimitiveType
            valueType <- genDataType

            allowNullable = params.allowNullableMapType
            valueNullable <- if (allowNullable) arbitrary[Boolean] else Gen.const(false)
          } yield MapType(keyType, valueType, valueNullable))
      }
  }

  def genTupleType: Gen[TupleType] = Gen.parameterized {
    case params: DataTypeParameters =>
      Gen.sized {
        case 0 =>
          Gen.fail

        case size =>
          Gen.resize(size - 1, for {
            upperBound <- Gen.size
            lowerBound = if (params.allowEmptyTupleType) 0 else 1

            fieldNum <- Gen.choose(lowerBound, upperBound)
            fieldTypeUpperBound = upperBound / (fieldNum min 1)

            genFieldType = Gen.resize(fieldTypeUpperBound, genDataType)
            allowNullable = params.allowNullableTupleField
            genNullable = if (allowNullable) arbitrary[Boolean] else Gen.const(false)

            genFieldSchema = for {
              fieldType <- genFieldType
              nullable <- genNullable
            } yield Schema(fieldType, nullable)

            fieldSchemas <- Gen.listOfN(fieldNum, genFieldSchema)

            fields = fieldSchemas.zipWithIndex map {
              case (Schema(dataType, nullable), ordinal) =>
                TupleField(s"c$ordinal", dataType, nullable)
            }
          } yield TupleType(fields))
      }
  }
}
