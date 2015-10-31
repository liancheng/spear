package scraper.generators

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.{ choose, const, listOfN, oneOf }
import org.scalacheck.{ Arbitrary, Gen }
import scraper.types._

package object types {
  lazy val genIntegralType: Gen[IntegralType] = oneOf(
    const(ByteType),
    const(ShortType),
    const(IntType),
    const(LongType)
  )

  implicit val arbIntegralType: Arbitrary[IntegralType] = Arbitrary(genIntegralType)

  lazy val genFractionalType: Gen[FractionalType] = oneOf(
    const(FloatType),
    const(DoubleType)
  )

  implicit val arbFractionalType: Arbitrary[FractionalType] = Arbitrary(genFractionalType)

  lazy val genNumericType: Gen[NumericType] = oneOf(
    genIntegralType,
    genFractionalType
  )

  implicit val arbNumericType: Arbitrary[NumericType] = Arbitrary(genNumericType)

  lazy val genPrimitiveType: Gen[PrimitiveType] = oneOf(
    genNumericType,
    const(StringType),
    const(BooleanType)
  )

  implicit val arbPrimitiveType: Arbitrary[PrimitiveType] = Arbitrary(genPrimitiveType)

  def genArrayType(implicit dim: DataTypeDim): Gen[ArrayType] = {
    require(dim.maxDepth >= 2, s"Max depth too small: ${dim.maxDepth}")
    require(dim.maxSize >= 2, s"Max size too small: ${dim.maxSize}")

    val genElementType = dim match {
      case DataTypeDim(maxDepth, maxSize) if (maxDepth min maxSize) == 2 =>
        // Size and depth of an `ArrayType` are at least 2
        genPrimitiveType

      case DataTypeDim(maxDepth, maxSize) =>
        genDataType(DataTypeDim(maxDepth - 1, maxSize - 1))
    }

    for {
      elementType <- genElementType
      elementNullable <- arbitrary[Boolean]
    } yield ArrayType(elementType, elementNullable)
  }

  implicit val arbArrayType: Arbitrary[ArrayType] = Arbitrary(genArrayType(defaultDataTypeDim))

  def genMapType(implicit dim: DataTypeDim): Gen[MapType] = {
    require(dim.maxDepth >= 2, s"Max depth too small: ${dim.maxDepth}")
    require(dim.maxSize >= 3, s"Max size too small: ${dim.maxSize}")

    val genValueType = dim match {
      case DataTypeDim(2, _) =>
        // Depth of a `MapType` is at least 2
        genPrimitiveType

      case DataTypeDim(_, 3) =>
        // Size of a `MapType` is at least 3
        genPrimitiveType

      case DataTypeDim(maxDepth, maxSize) =>
        genDataType(DataTypeDim(maxDepth - 1, maxSize - 2))
    }

    for {
      keyType <- genPrimitiveType
      valueType <- genValueType
      valueNullable <- arbitrary[Boolean]
    } yield MapType(keyType, valueType, valueNullable)
  }

  implicit val arbMapType: Arbitrary[MapType] = Arbitrary(genMapType(defaultDataTypeDim))

  def genStructType(implicit dim: DataTypeDim): Gen[StructType] = {
    require(dim.maxDepth >= 2, s"Max depth too small: ${dim.maxDepth}")
    require(dim.maxSize >= 2, s"Max size too small: ${dim.maxSize}")

    for {
      fieldNum <- choose(1, dim.maxSize - 1)
      maxFieldSize = (dim.maxSize - 1) / fieldNum

      genFieldSchema = for {
        dataType <- genDataType(DataTypeDim(dim.maxDepth - 1, maxFieldSize))
        nullable <- arbitrary[Boolean]
      } yield Schema(dataType, nullable)

      fieldSchemata <- listOfN(fieldNum, genFieldSchema)
    } yield {
      StructType(fieldSchemata.zipWithIndex.map {
        case (schema, ordinal) =>
          StructField(s"col$ordinal", schema)
      })
    }
  }

  implicit val arbStructType: Arbitrary[StructType] = Arbitrary(genStructType(defaultDataTypeDim))

  def genComplexType(implicit dim: DataTypeDim): Gen[ComplexType] = {
    require(dim.maxDepth >= 2, s"Max depth too small: ${dim.maxDepth}")
    require(dim.maxSize >= 2, s"Max size too small: ${dim.maxSize}")

    dim match {
      case DataTypeDim(_, 2) => oneOf(genArrayType(dim), genStructType(dim))
      case _                 => oneOf(genArrayType(dim), genStructType(dim), genMapType(dim))
    }
  }

  implicit val arbComplexType: Arbitrary[ComplexType] =
    Arbitrary(genComplexType(defaultDataTypeDim))

  def genDataType(implicit dim: DataTypeDim): Gen[DataType] = {
    require(dim.maxDepth >= 1, s"Max depth too small: ${dim.maxDepth}")
    require(dim.maxSize >= 1, s"Max size too small: ${dim.maxSize}")

    dim match {
      case DataTypeDim(1, _) =>
        genPrimitiveType

      case DataTypeDim(_, 1) =>
        genPrimitiveType

      case DataTypeDim(maxDepth, maxSize) =>
        Gen.frequency(
          3 -> genPrimitiveType,
          7 -> genComplexType(DataTypeDim(maxDepth, maxSize))
        )
    }
  }

  implicit val arbDataType: Arbitrary[DataType] = Arbitrary(genDataType(defaultDataTypeDim))
}
