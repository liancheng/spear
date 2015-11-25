package scraper.generators

import scala.collection.JavaConverters._

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import scraper.Row
import scraper.config.Settings
import scraper.config.Settings.Key
import scraper.types._

package object values {
  val MaxRepetition: Key[Int] = Key("scraper.test.expressions.max-repetition").int

  def genValueForDataType(dataType: DataType)(implicit settings: Settings): Gen[Any] =
    dataType match {
      case t: PrimitiveType => genValueForPrimitiveType(t)
      case t: ComplexType   => genValueForComplexType(t)(settings)
    }

  def genValueForPrimitiveType(dataType: PrimitiveType): Gen[Any] = dataType match {
    case NullType       => Gen.const(null)
    case BooleanType    => arbitrary[Boolean]
    case StringType     => arbitrary[String]
    case t: NumericType => genValueForNumericType(t)
  }

  def genValueForNumericType(dataType: NumericType): Gen[Any] = dataType match {
    case t: IntegralType   => genValueForIntegralType(t)
    case t: FractionalType => genValueForFractionalType(t)
  }

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

  def genValueForComplexType(dataType: ComplexType)(implicit settings: Settings): Gen[Any] =
    dataType match {
      case t: ArrayType => genValueForArrayType(t)(settings)
      case t: MapType   => genValueForMapType(t)(settings)
      case t: TupleType => genValueForTupleType(t)(settings)
    }

  def genValueForArrayType(dataType: ArrayType)(implicit settings: Settings): Gen[Seq[_]] = {
    val genElement = if (dataType.elementNullable) {
      Gen.option(genValueForDataType(dataType)(settings)) map (_.orNull)
    } else {
      genValueForDataType(dataType)(settings)
    }

    for {
      repetition <- Gen.choose(0, settings(MaxRepetition))
      elements <- Gen.listOfN(repetition, genElement)
    } yield elements
  }

  def genValueForMapType(dataType: MapType)(implicit settings: Settings): Gen[Map[_, _]] = {
    val genKey = genValueForDataType(dataType.keyType)(settings)
    val genValue = {
      val genNonNullValue = genValueForDataType(dataType)(settings)
      if (dataType.valueNullable) Gen.option(genNonNullValue) map (_.orNull) else genNonNullValue
    }

    for {
      repetition <- Gen.choose(0, settings(MaxRepetition))
      keys <- Gen.listOfN(repetition, genKey)
      values <- Gen.listOfN(repetition, genValue)
    } yield (keys zip values).toMap
  }

  def genValueForTupleType(dataType: TupleType)(implicit settings: Settings): Gen[Row] = {
    val genFields = Gen.sequence(dataType.fields map {
      case TupleField(_, fieldType, nullable) =>
        val genNonNullField = genValueForDataType(fieldType)(settings)
        if (nullable) Gen.option(genNonNullField) map (_.orNull) else genNonNullField
    })

    for (fields <- genFields) yield new Row(fields.asScala)
  }
}
