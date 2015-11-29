package scraper.expressions

import scala.util.{Failure, Success, Try}

import scraper._
import scraper.expressions.Cast.{buildCast, convertible}
import scraper.types._

case class Cast(child: Expression, toType: DataType) extends UnaryExpression {
  override def dataType: DataType = toType

  override def annotatedString: String =
    s"CAST(${child.annotatedString} AS ${toType.simpleName})"

  private def fromType = child.dataType

  override def evaluate(input: Row): Any =
    buildCast(fromType)(toType)(child evaluate input)

  override lazy val strictlyTypedForm: Try[Expression] = for {
    strictChild <- child.strictlyTypedForm map {
      case e if convertible(e.dataType, toType) => e
      case e                                    => throw new TypeCastException(e.dataType, toType)
    }
  } yield if (strictChild sameOrEqual child) this else this.copy(child = strictChild)

  override def sql: String = s"CAST(${child.sql} AS ${dataType.sql})"
}

object Cast {
  private type CastBuilder = PartialFunction[DataType, Any => Any]

  private val implicitlyFromBoolean: CastBuilder = {
    case StringType => _.toString
  }

  private val asBoolean = (_: Any) match { case v: Boolean => v }

  private val fromBoolean: CastBuilder = implicitlyFromBoolean orElse {
    case IntType => asBoolean andThen (if (_) 1 else 0)
  }

  private val asByte = (_: Any) match { case v: Byte => v }

  private val implicitlyFromByte: CastBuilder = {
    case ShortType  => asByte andThen (_.toShort)
    case IntType    => asByte andThen (_.toInt)
    case LongType   => asByte andThen (_.toLong)
    case FloatType  => asByte andThen (_.toFloat)
    case DoubleType => asByte andThen (_.toDouble)
    case StringType => _.toString
  }

  private val fromByte: CastBuilder = implicitlyFromByte

  private val asShort = (_: Any) match { case v: Short => v }

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

  private val asInt = (_: Any) match { case v: Int => v }

  private val implicitlyFromInt: CastBuilder = {
    case BooleanType => asInt andThen (_ == 0)
    case LongType    => asInt andThen (_.toLong)
    case FloatType   => asInt andThen (_.toFloat)
    case DoubleType  => asInt andThen (_.toDouble)
    case StringType  => _.toString
  }

  private val fromInt: CastBuilder = implicitlyFromInt orElse {
    case ByteType  => asInt andThen (_.toByte)
    case ShortType => asInt andThen (_.toShort)
  }

  private val asLong = (_: Any) match { case v: Long => v }

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

  private val asFloat = (_: Any) match { case v: Float => v }

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

  private val asDouble = (_: Any) match { case v: Double => v }

  private val fromDouble: CastBuilder = implicitlyFromFloat orElse {
    case ByteType  => asDouble andThen (_.toByte)
    case ShortType => asDouble andThen (_.toShort)
    case IntType   => asDouble andThen (_.toInt)
    case LongType  => asDouble andThen (_.toLong)
    case FloatType => asDouble andThen (_.toFloat)
  }

  private val booleanStrings = Set("yes", "no", "y", "n", "true", "false", "t", "f", "on", "off")

  private val asString = (_: Any) match { case v: String => v }

  private val implicitlyFromString: CastBuilder = {
    case BooleanType => asString andThen booleanStrings.contains
    case ByteType    => asString andThen (_.toByte)
    case ShortType   => asString andThen (_.toShort)
    case IntType     => asString andThen (_.toInt)
    case LongType    => asString andThen (_.toLong)
    case FloatType   => asString andThen (_.toFloat)
    case DoubleType  => asString andThen (_.toDouble)
  }

  private val fromString: CastBuilder = implicitlyFromString

  private def buildImplicitCast(from: DataType): CastBuilder = from match {
    case BooleanType => implicitlyFromBoolean
    case ByteType    => implicitlyFromByte
    case ShortType   => implicitlyFromShort
    case IntType     => implicitlyFromInt
    case LongType    => implicitlyFromLong
    case FloatType   => implicitlyFromFloat
    case DoubleType  => implicitlyFromDouble
    case StringType  => implicitlyFromString
  }

  private def buildCast(from: DataType): CastBuilder = from match {
    case BooleanType => fromBoolean
    case ByteType    => fromByte
    case ShortType   => fromShort
    case IntType     => fromInt
    case LongType    => fromLong
    case FloatType   => fromFloat
    case DoubleType  => fromDouble
    case StringType  => fromString
  }

  /**
   * Whether [[DataType]] `x` can be converted to [[DataType]] `y` implicitly.
   *
   * @note Any [[DataType]] is NOT considered to be [[implicitlyConvertible]] to itself.
   */
  def implicitlyConvertible(x: DataType, y: DataType): Boolean = buildImplicitCast(x) isDefinedAt y

  /**
   * Whether [[DataType]] `x` can be converted to [[DataType]] `y`, either implicitly or
   * explicitly.
   *
   * @note Any [[DataType]] is NOT considered to be [[convertible]] to itself.
   */
  def convertible(x: DataType, y: DataType): Boolean = buildCast(x) isDefinedAt y

  /**
   * [[DataType]] `x` is implicitly compatible with [[DataType]] `y` iff:
   *
   *  - `x == y`, or
   *  - `x` is [[implicitlyConvertible]] to `y`
   */
  def implicitlyCompatible(x: DataType, y: DataType): Boolean =
    x == y || implicitlyConvertible(x, y) || implicitlyConvertible(y, x)

  /**
   * Returns a new [[Expression]] that [[Cast]]s [[Expression]] `e` to `dataType` if the
   * [[DataType]] of `e` is [[implicitlyConvertible]] to `dataType`.
   */
  def promoteDataType(e: Expression, dataType: DataType): Expression = e match {
    case _ if e.dataType == dataType                      => e
    case _ if implicitlyConvertible(e.dataType, dataType) => e cast dataType
    case _ =>
      throw new ImplicitCastException(e.dataType, dataType)
  }

  def commonTypeOf(first: DataType, rest: DataType*): Try[DataType] =
    rest.foldLeft(Try(first)) {
      case (Success(x), y) if implicitlyCompatible(x, y) => Try(y)
      case (Success(x), y) if implicitlyCompatible(y, x) => Try(x)
      case _ =>
        Failure(new TypeMismatchException(
          s"Could not find common type for: ${first +: rest map (_.sql) mkString ", "}", None
        ))
    }
}
