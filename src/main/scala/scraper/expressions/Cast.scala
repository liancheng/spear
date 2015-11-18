package scraper.expressions

import scraper.expressions.Cast.explicitlyCastable
import scraper.types._
import scraper.{ Row, TypeCastError }

case class Cast(fromValue: Expression, toType: DataType) extends UnaryExpression {
  override def child: Expression = fromValue

  override def dataType: DataType = toType

  private def fromType = fromValue.dataType

  private def cast: Any => Any = fromType match {
    case ByteType   => fromByte(toType)
    case ShortType  => fromShort(toType)
    case IntType    => fromInt(toType)
    case LongType   => fromLong(toType)
    case FloatType  => fromFloat(toType)
    case DoubleType => fromDouble(toType)
    case StringType => fromString(toType)
  }

  private def fromByte(to: DataType): Any => Any = {
    val asByte = (_: Any).asInstanceOf[Byte]
    to match {
      case ShortType  => asByte andThen (_.toShort)
      case IntType    => asByte andThen (_.toInt)
      case LongType   => asByte andThen (_.toLong)
      case FloatType  => asByte andThen (_.toFloat)
      case DoubleType => asByte andThen (_.toDouble)
      case StringType => (_: Any).toString
      case ByteType   => identity
      case _          => throw TypeCastError(fromType, to)
    }
  }

  private def fromShort(to: DataType): Any => Any = {
    val asShort = (_: Any).asInstanceOf[Short]
    to match {
      case ByteType   => asShort andThen (_.toByte)
      case IntType    => asShort andThen (_.toInt)
      case LongType   => asShort andThen (_.toLong)
      case FloatType  => asShort andThen (_.toFloat)
      case DoubleType => asShort andThen (_.toDouble)
      case StringType => _.toString
      case ShortType  => identity
      case _          => throw TypeCastError(fromType, to)
    }
  }

  private def fromInt(to: DataType): Any => Any = {
    val asInt = (_: Any).asInstanceOf[Int]
    to match {
      case ByteType   => asInt andThen (_.toByte)
      case ShortType  => asInt andThen (_.toShort)
      case LongType   => asInt andThen (_.toLong)
      case FloatType  => asInt andThen (_.toFloat)
      case DoubleType => asInt andThen (_.toDouble)
      case StringType => _.toString
      case IntType    => identity
      case _          => throw TypeCastError(fromType, to)
    }
  }

  private def fromLong(to: DataType): Any => Any = {
    val asLong = (_: Any) match { case v: Long => v }
    to match {
      case ByteType   => asLong andThen (_.toByte)
      case ShortType  => asLong andThen (_.toShort)
      case IntType    => asLong andThen (_.toInt)
      case FloatType  => asLong andThen (_.toFloat)
      case DoubleType => asLong andThen (_.toDouble)
      case StringType => _.toString
      case LongType   => identity
      case _          => throw TypeCastError(fromType, to)
    }
  }

  private def fromFloat(to: DataType): Any => Any = {
    val asFloat = (_: Any).asInstanceOf[Float]
    to match {
      case ByteType   => asFloat andThen (_.toByte)
      case ShortType  => asFloat andThen (_.toShort)
      case IntType    => asFloat andThen (_.toInt)
      case LongType   => asFloat andThen (_.toLong)
      case DoubleType => asFloat andThen (_.toDouble)
      case StringType => _.toString
      case FloatType  => identity
      case _          => throw TypeCastError(fromType, to)
    }
  }

  private def fromDouble(to: DataType): Any => Any = {
    val asDouble = (_: Any).asInstanceOf[Double]
    to match {
      case ByteType   => asDouble andThen (_.toByte)
      case ShortType  => asDouble andThen (_.toShort)
      case IntType    => asDouble andThen (_.toInt)
      case LongType   => asDouble andThen (_.toFloat)
      case FloatType  => asDouble andThen (_.toFloat)
      case StringType => _.toString
      case DoubleType => identity
      case _          => throw TypeCastError(fromType, to)
    }
  }

  private def fromString(to: DataType): Any => Any = {
    val asString = (_: Any).asInstanceOf[String]
    to match {
      case ByteType   => asString andThen java.lang.Byte.valueOf
      case ShortType  => asString andThen java.lang.Short.valueOf
      case IntType    => asString andThen java.lang.Integer.valueOf
      case LongType   => asString andThen java.lang.Long.valueOf
      case FloatType  => asString andThen java.lang.Float.valueOf
      case DoubleType => asString andThen java.lang.Double.valueOf
      case StringType => identity
      case _          => throw TypeCastError(fromType, to)
    }
  }

  override def evaluate(input: Row): Any = cast(fromValue.evaluate(input))

  override def typeChecked: Boolean =
    child.typeChecked && explicitlyCastable(child.dataType, toType)
}

object Cast {
  def implicitlyCastable(from: DataType, to: DataType): Boolean = (from, to) match {
    case _ if from == to                            => true
    case (_, StringType)                            => true
    case (_: IntegralType, _: FractionalType)       => true
    case (IntType, LongType)                        => true
    case (ByteType, ShortType | IntType | LongType) => true
    case (ShortType, IntType | LongType)            => true
    case (FloatType, DoubleType)                    => true
    case _                                          => false
  }

  def explicitlyCastable(from: DataType, to: DataType): Boolean = (from, to) match {
    case _ if implicitlyCastable(from, to) => true
    case (_: NumericType, _: NumericType)  => true
    case _                                 => false
  }
}
