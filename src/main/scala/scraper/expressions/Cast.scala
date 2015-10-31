package scraper.expressions

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

  private def fromByte(to: DataType): Any => Any = to match {
    case ByteType   => identity
    case ShortType  => (_: Any).asInstanceOf[Byte].toShort
    case IntType    => (_: Any).asInstanceOf[Byte].toInt
    case LongType   => (_: Any).asInstanceOf[Byte].toLong
    case FloatType  => (_: Any).asInstanceOf[Byte].toFloat
    case DoubleType => (_: Any).asInstanceOf[Byte].toDouble
    case StringType => (_: Any).toString
    case _          => throw TypeCastError(fromType, to)
  }

  private def fromShort(to: DataType): Any => Any = to match {
    case ByteType   => (_: Any).asInstanceOf[Short].toByte
    case ShortType  => identity
    case IntType    => (_: Any).asInstanceOf[Short].toInt
    case LongType   => (_: Any).asInstanceOf[Short].toLong
    case FloatType  => (_: Any).asInstanceOf[Short].toFloat
    case DoubleType => (_: Any).asInstanceOf[Short].toDouble
    case StringType => (_: Any).toString
    case _          => throw TypeCastError(fromType, to)
  }

  private def fromInt(to: DataType): Any => Any = to match {
    case ByteType   => (_: Any).asInstanceOf[Int].toByte
    case ShortType  => (_: Any).asInstanceOf[Int].toShort
    case IntType    => identity
    case LongType   => (_: Any).asInstanceOf[Int].toLong
    case FloatType  => (_: Any).asInstanceOf[Int].toFloat
    case DoubleType => (_: Any).asInstanceOf[Int].toDouble
    case StringType => (_: Any).toString
    case _          => throw TypeCastError(fromType, to)
  }

  private def fromLong(to: DataType): Any => Any = to match {
    case ByteType   => (_: Any).asInstanceOf[Long].toByte
    case ShortType  => (_: Any).asInstanceOf[Long].toShort
    case IntType    => (_: Any).asInstanceOf[Long].toInt
    case LongType   => identity
    case FloatType  => (_: Any).asInstanceOf[Long].toFloat
    case DoubleType => (_: Any).asInstanceOf[Long].toDouble
    case StringType => (_: Any).toString
    case _          => throw TypeCastError(fromType, to)
  }

  private def fromFloat(to: DataType): Any => Any = to match {
    case ByteType   => (_: Any).asInstanceOf[Float].toByte
    case ShortType  => (_: Any).asInstanceOf[Float].toShort
    case IntType    => (_: Any).asInstanceOf[Float].toInt
    case LongType   => (_: Any).asInstanceOf[Float].toLong
    case FloatType  => identity
    case DoubleType => (_: Any).asInstanceOf[Float].toDouble
    case StringType => (_: Any).toString
    case _          => throw TypeCastError(fromType, to)
  }

  private def fromDouble(to: DataType): Any => Any = to match {
    case ByteType   => (_: Any).asInstanceOf[Double].toByte
    case ShortType  => (_: Any).asInstanceOf[Double].toShort
    case IntType    => (_: Any).asInstanceOf[Double].toInt
    case LongType   => (_: Any).asInstanceOf[Double].toLong
    case FloatType  => (_: Any).asInstanceOf[Double].toFloat
    case DoubleType => identity
    case StringType => (_: Any).toString
    case _          => throw TypeCastError(fromType, to)
  }

  private def fromString(to: DataType): Any => Any = to match {
    // format: OFF
    case ByteType   => (value: Any) => java.lang.Byte.valueOf(value.asInstanceOf[String])
    case ShortType  => (value: Any) => java.lang.Short.valueOf(value.asInstanceOf[String])
    case IntType    => (value: Any) => java.lang.Integer.valueOf(value.asInstanceOf[String])
    case LongType   => (value: Any) => java.lang.Long.valueOf(value.asInstanceOf[String])
    case FloatType  => (value: Any) => java.lang.Float.valueOf(value.asInstanceOf[String])
    case DoubleType => (value: Any) => java.lang.Double.valueOf(value.asInstanceOf[String])
    case StringType => identity
    case _          => throw TypeCastError(fromType, to)
    // format: ON
  }

  override def evaluate(input: Row): Any = cast(fromValue.evaluate(input))
}

object Cast {
  def implicitlyCastable(from: DataType, to: DataType): Boolean = (from, to) match {
    // format: OFF
    case (_, StringType)                            => true
    case (_: IntegralType, _: FractionalType)       => true
    case (IntType, LongType)                        => true
    case (ByteType, ShortType | IntType | LongType) => true
    case (ShortType, IntType | LongType)            => true
    case (FloatType, DoubleType)                    => true
    case _                                          => false
    // format: ON
  }

  def explicitlyCastable(from: DataType, to: DataType): Boolean = (from, to) match {
    case _ if implicitlyCastable(from, to) => true
    case (_: NumericType, _: NumericType)  => true
    case _                                 => false
  }
}
