package scraper.expressions

import scala.util.{Failure, Success, Try}

import scraper.types._
import scraper.{TypeCheckException, Row, TypeCastException}

case class Cast(fromExpression: Expression, toType: DataType) extends UnaryExpression {
  override def child: Expression = fromExpression

  override def dataType: DataType = toType

  override def annotatedString: String =
    s"CAST(${child.annotatedString} AS ${toType.simpleName})"

  private def fromType = fromExpression.dataType

  override def evaluate(input: Row): Any =
    Cast.buildCast(fromType)(toType)(fromExpression evaluate input)

  override lazy val strictlyTyped: Try[Expression] = {
    val strictChild = child.strictlyTyped.recover {
      case cause: Throwable =>
        throw TypeCheckException(child, Some(cause))
    }

    strictChild map {
      case e if e sameOrEqual child => this
      case e                        => copy(fromExpression = e)
    }
  }

  override def sql: String = s"CAST(${child.sql} AS ${dataType.sql})"
}

object Cast {
  private type CastBuilder = PartialFunction[DataType, Any => Any]

  private val asByte = (_: Any) match { case v: Byte => v }
  private val asShort = (_: Any) match { case v: Short => v }
  private val asInt = (_: Any) match { case v: Int => v }
  private val asLong = (_: Any) match { case v: Long => v }
  private val asFloat = (_: Any) match { case v: Float => v }
  private val asDouble = (_: Any) match { case v: Double => v }

  private val implicitlyFromByte: CastBuilder = {
    case ShortType  => asByte andThen (_.toShort)
    case IntType    => asByte andThen (_.toInt)
    case LongType   => asByte andThen (_.toLong)
    case FloatType  => asByte andThen (_.toFloat)
    case DoubleType => asByte andThen (_.toDouble)
    case StringType => _.toString
  }

  private val fromByte: CastBuilder = implicitlyFromByte

  private val implicitlyFromShort: CastBuilder = {
    case IntType    => asShort andThen (_.toInt)
    case LongType   => asShort andThen (_.toLong)
    case FloatType  => asShort andThen (_.toFloat)
    case DoubleType => asShort andThen (_.toDouble)
    case StringType => _.toString
  }

  private val fromShort: CastBuilder = implicitlyFromShort orElse {
    case ByteType => asShort andThen (_.toByte)
  }

  private val implicitlyFromInt: CastBuilder = {
    case LongType   => asInt andThen (_.toLong)
    case FloatType  => asInt andThen (_.toFloat)
    case DoubleType => asInt andThen (_.toDouble)
    case StringType => _.toString
  }

  private val fromInt: CastBuilder = implicitlyFromInt orElse {
    case ByteType  => asInt andThen (_.toByte)
    case ShortType => asInt andThen (_.toShort)
  }

  private val implicitlyFromLong: CastBuilder = {
    case FloatType  => asLong andThen (_.toFloat)
    case DoubleType => asLong andThen (_.toDouble)
    case StringType => _.toString
  }

  private val fromLong: CastBuilder = implicitlyFromLong orElse {
    case ByteType  => asLong andThen (_.toByte)
    case ShortType => asLong andThen (_.toShort)
    case IntType   => asLong andThen (_.toInt)
  }

  private val implicitlyFromFloat: CastBuilder = {
    case DoubleType => asFloat andThen (_.toDouble)
    case StringType => _.toString
  }

  private val fromFloat: CastBuilder = implicitlyFromFloat orElse {
    case ByteType  => asFloat andThen (_.toByte)
    case ShortType => asFloat andThen (_.toShort)
    case IntType   => asFloat andThen (_.toInt)
    case LongType  => asFloat andThen (_.toLong)
  }

  private val implicitlyFromDouble: CastBuilder = {
    case StringType => _.toString
  }

  private val fromDouble: CastBuilder = implicitlyFromFloat orElse {
    case ByteType  => asDouble andThen (_.toByte)
    case ShortType => asDouble andThen (_.toShort)
    case IntType   => asDouble andThen (_.toInt)
    case LongType  => asDouble andThen (_.toLong)
    case FloatType => asDouble andThen (_.toFloat)
  }

  private def buildImplicitCast(from: DataType): CastBuilder = from match {
    case ByteType   => implicitlyFromByte
    case ShortType  => implicitlyFromShort
    case IntType    => implicitlyFromInt
    case LongType   => implicitlyFromLong
    case FloatType  => implicitlyFromFloat
    case DoubleType => implicitlyFromDouble
  }

  private def buildCast(from: DataType): CastBuilder = from match {
    case ByteType   => fromByte
    case ShortType  => fromShort
    case IntType    => fromInt
    case LongType   => fromLong
    case FloatType  => fromFloat
    case DoubleType => fromDouble
  }

  /** Whether [[DataType]] `x` can be converted to [[DataType]] `y` implicitly. */
  def implicitlyConvertible(x: DataType, y: DataType): Boolean = buildImplicitCast(x) isDefinedAt y

  /**
   * Whether [[DataType]] `x` can be converted to [[DataType]] `y`, either implicitly or
   * explicitly.
   */
  def convertible(x: DataType, y: DataType): Boolean = buildCast(x) isDefinedAt y

  def implicitlyCompatible(x: DataType, y: DataType): Boolean =
    x == y || implicitlyConvertible(x, y) || implicitlyConvertible(y, x)

  def promoteDataTypes(e1: Expression, e2: Expression): Try[(Expression, Expression)] =
    (e1.dataType, e2.dataType) match {
      case (t1, t2) if t1 == t2                      => Success((e1, e2))
      case (t1, t2) if implicitlyConvertible(t1, t2) => Success((Cast(e1, t2), e2))
      case (t1, t2) if implicitlyConvertible(t2, t1) => Success((e1, Cast(e2, t1)))
      case (t1, t2)                                  => Failure(TypeCastException(t1, t2))
    }
}
