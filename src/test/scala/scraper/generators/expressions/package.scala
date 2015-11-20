package scraper.generators

import scala.collection.JavaConverters._

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalacheck.Gen.{choose, listOfN, option, sequence}
import scraper.Row
import scraper.types._

package object expressions {
  def genValueForIntegralType(dataType: IntegralType): Gen[Any] = dataType match {
    case ByteType  => arbitrary[Byte]
    case ShortType => arbitrary[Short]
    case IntType   => arbitrary[Int]
    case LongType  => arbitrary[Long]
  }

  def genValueForFractionalType(dataType: FractionalType): Gen[Any] = dataType match {
    case FloatType  => arbitrary[Float]
    case DoubleType => arbitrary[Double]
  }

  def genValueForNumericType(dataType: NumericType): Gen[Any] = dataType match {
    case t: IntegralType   => genValueForIntegralType(t)
    case t: FractionalType => genValueForFractionalType(t)
  }

  def genValueForPrimitiveType(dataType: PrimitiveType): Gen[Any] = dataType match {
    case BooleanType    => arbitrary[Boolean]
    case StringType     => arbitrary[String]
    case t: NumericType => genValueForNumericType(t)
  }

  def genValueForArrayType(dataType: ArrayType)(implicit dim: ValueDim): Gen[Seq[_]] = {
    val genElement = if (dataType.elementNullable) {
      for {
        maybeElement <- option(genValueForDataType(dataType)(dim))
      } yield maybeElement.orNull
    } else {
      genValueForDataType(dataType)(dim)
    }

    for {
      repetition <- choose(0, dim.maxRepetition)
      elements <- listOfN(repetition, genElement)
    } yield elements
  }

  def genValueForMapType(dataType: MapType)(implicit dim: ValueDim): Gen[Map[_, _]] = {
    val genKey = genValueForDataType(dataType.keyType)(dim)
    val genValue = {
      val genNonNullValue = genValueForDataType(dataType)(dim)
      if (dataType.valueNullable) option(genNonNullValue).map(_.orNull)
      else genNonNullValue
    }

    for {
      repetition <- choose(0, dim.maxRepetition)
      keys <- listOfN(repetition, genKey)
      values <- listOfN(repetition, genValue)
    } yield (keys zip values).toMap
  }

  def genValueForTupleType(dataType: TupleType)(implicit dim: ValueDim): Gen[Row] = {
    val genFields = sequence(dataType.fields.map {
      case TupleField(_, fieldType, nullable) =>
        val genNonNullField = genValueForDataType(fieldType)(dim)
        if (nullable) option(genNonNullField).map(_.orNull)
        else genNonNullField
    })

    for {
      fields <- genFields
    } yield new Row(fields.asScala)
  }

  def genValueForComplexType(dataType: ComplexType)(implicit dim: ValueDim): Gen[Any] =
    dataType match {
      case t: ArrayType => genValueForArrayType(t)(dim)
      case t: MapType   => genValueForMapType(t)(dim)
      case t: TupleType => genValueForTupleType(t)(dim)
    }

  def genValueForDataType(dataType: DataType)(implicit dim: ValueDim): Gen[Any] = dataType match {
    case t: PrimitiveType => genValueForPrimitiveType(t)
    case t: ComplexType   => genValueForComplexType(t)(dim)
  }
}
