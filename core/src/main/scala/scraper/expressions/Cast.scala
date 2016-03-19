package scraper.expressions

import scala.util.{Success, Try}

import scraper.exceptions.{ImplicitCastException, TypeCastException}
import scraper.expressions.Cast.{buildCast, convertible}
import scraper.types
import scraper.types._

case class Cast(child: Expression, override val dataType: DataType) extends UnaryExpression {
  override protected def template(childString: String): String =
    s"CAST($childString AS ${dataType.sql})"

  private def fromType = child.dataType

  override def nullSafeEvaluate(value: Any): Any =
    buildCast(fromType, dataType).get(value)

  override lazy val strictlyTyped: Try[Expression] = for {
    strictChild <- child.strictlyTyped map {
      case e if convertible(e.dataType, dataType) => e
      case e =>
        throw new TypeCastException(e.dataType, dataType)
    }
  } yield if (strictChild same child) this else this.copy(child = strictChild)
}

object Cast {
  private val implicitlyFromBoolean: PartialFunction[DataType, Any => Any] = {
    case StringType => _.toString
  }

  private val asBoolean = (_: Any) match { case v: Boolean => v }

  private val explicitlyFromBoolean: PartialFunction[DataType, Any => Any] = {
    case IntType => asBoolean andThen (if (_) 1 else 0)
  }

  private val asByte = (_: Any) match { case v: Byte => v }

  private val implicitlyFromByte: PartialFunction[DataType, Any => Any] = {
    case ShortType  => asByte andThen (_.toShort)
    case IntType    => asByte andThen (_.toInt)
    case LongType   => asByte andThen (_.toLong)
    case FloatType  => asByte andThen (_.toFloat)
    case DoubleType => asByte andThen (_.toDouble)
    case StringType => _.toString
  }

  private val explicitlyFromByte: PartialFunction[DataType, Any => Any] = {
    case _ if false => identity
  }

  private val asShort = (_: Any) match { case v: Short => v }

  private val implicitlyFromShort: PartialFunction[DataType, Any => Any] = {
    case IntType    => asShort andThen (_.toInt)
    case LongType   => asShort andThen (_.toLong)
    case FloatType  => asShort andThen (_.toFloat)
    case DoubleType => asShort andThen (_.toDouble)
    case StringType => _.toString
  }

  private val explicitlyFromShort: PartialFunction[DataType, Any => Any] = {
    case ByteType => asShort andThen (_.toByte)
  }

  private val asInt = (_: Any) match { case v: Int => v }

  private val implicitlyFromInt: PartialFunction[DataType, Any => Any] = {
    case BooleanType => asInt andThen (_ == 0)
    case LongType    => asInt andThen (_.toLong)
    case FloatType   => asInt andThen (_.toFloat)
    case DoubleType  => asInt andThen (_.toDouble)
    case StringType  => _.toString
  }

  private val explicitlyFromInt: PartialFunction[DataType, Any => Any] = {
    case ByteType  => asInt andThen (_.toByte)
    case ShortType => asInt andThen (_.toShort)
  }

  private val asLong = (_: Any) match { case v: Long => v }

  private val implicitlyFromLong: PartialFunction[DataType, Any => Any] = {
    case FloatType  => asLong andThen (_.toFloat)
    case DoubleType => asLong andThen (_.toDouble)
    case StringType => _.toString
  }

  private val explicitlyFromLong: PartialFunction[DataType, Any => Any] = {
    case ByteType  => asLong andThen (_.toByte)
    case ShortType => asLong andThen (_.toShort)
    case IntType   => asLong andThen (_.toInt)
  }

  private val asFloat = (_: Any) match { case v: Float => v }

  private val implicitlyFromFloat: PartialFunction[DataType, Any => Any] = {
    case DoubleType => asFloat andThen (_.toDouble)
    case StringType => _.toString
  }

  private val explicitlyFromFloat: PartialFunction[DataType, Any => Any] = {
    case ByteType  => asFloat andThen (_.toByte)
    case ShortType => asFloat andThen (_.toShort)
    case IntType   => asFloat andThen (_.toInt)
    case LongType  => asFloat andThen (_.toLong)
  }

  private val implicitlyFromDouble: PartialFunction[DataType, Any => Any] = {
    case StringType => _.toString
  }

  private val asDouble = (_: Any) match { case v: Double => v }

  private val explicitlyFromDouble: PartialFunction[DataType, Any => Any] = {
    case ByteType  => asDouble andThen (_.toByte)
    case ShortType => asDouble andThen (_.toShort)
    case IntType   => asDouble andThen (_.toInt)
    case LongType  => asDouble andThen (_.toLong)
    case FloatType => asDouble andThen (_.toFloat)
  }

  private val booleanStrings = Set("yes", "no", "y", "n", "true", "false", "t", "f", "on", "off")

  private val asString = (_: Any) match { case v: String => v }

  private val implicitlyFromString: PartialFunction[DataType, Any => Any] = {
    case BooleanType => asString andThen (_.toLowerCase) andThen booleanStrings.contains
    case ByteType    => asString andThen (_.toByte)
    case ShortType   => asString andThen (_.toShort)
    case IntType     => asString andThen (_.toInt)
    case LongType    => asString andThen (_.toLong)
    case FloatType   => asString andThen (_.toFloat)
    case DoubleType  => asString andThen (_.toDouble)
  }

  private val explicitlyFromString: PartialFunction[DataType, Any => Any] = {
    case _ if false => identity
  }

  private val implicitlyFromNull: PartialFunction[DataType, Any => Any] = {
    // NullType can be casted to any other type, and only has a single value `null`.
    case _ => { case _ => null }
  }

  private val explicitlyFromNull: PartialFunction[DataType, Any => Any] = {
    case _ if false => identity
  }

  private type CastBuilder = PartialFunction[DataType, PartialFunction[DataType, Any => Any]]

  private val buildImplicitCast: CastBuilder = {
    case BooleanType => implicitlyFromBoolean
    case ByteType    => implicitlyFromByte
    case ShortType   => implicitlyFromShort
    case IntType     => implicitlyFromInt
    case LongType    => implicitlyFromLong
    case FloatType   => implicitlyFromFloat
    case DoubleType  => implicitlyFromDouble
    case StringType  => implicitlyFromString
    case NullType    => implicitlyFromNull
  }

  private val buildExplicitCast: CastBuilder = {
    case BooleanType => explicitlyFromBoolean
    case ByteType    => explicitlyFromByte
    case ShortType   => explicitlyFromShort
    case IntType     => explicitlyFromInt
    case LongType    => explicitlyFromLong
    case FloatType   => explicitlyFromFloat
    case DoubleType  => explicitlyFromDouble
    case StringType  => explicitlyFromString
    case NullType    => explicitlyFromNull
  }

  private def buildCast(x: DataType, y: DataType): Option[Any => Any] = {
    val maybeImplicitCast = buildImplicitCast lift x flatMap (_ lift y)
    val maybeExplicitCast = buildExplicitCast lift x flatMap (_ lift y)
    maybeImplicitCast orElse maybeExplicitCast
  }

  /**
   * Returns whether type `x` can be converted to type `y` implicitly.
   *
   * @note Any [[types.DataType DataType]] is considered to be [[implicitlyConvertible]] to itself.
   */
  def implicitlyConvertible(x: DataType, y: DataType): Boolean =
    x == y || buildImplicitCast.lift(x).exists(_ isDefinedAt y)

  /**
   * Returns whether type `x` can be converted to type `y`, either implicitly or explicitly.
   *
   * @note Any [[types.DataType DataType]] is considered to be [[convertible]] to itself.
   */
  def convertible(x: DataType, y: DataType): Boolean =
    x == y || buildCast(x, y).isDefined

  /**
   * Returns a new [[Expression]] that [[Cast]]s [[Expression]] `e` to `dataType` if the
   * [[types.DataType DataType]] of `e` is [[implicitlyConvertible]] to `dataType`.  If `e` is
   * already of the target type, `e` is returned untouched.
   */
  def promoteDataType(e: Expression, dataType: DataType): Expression = e match {
    case _ if e.dataType == dataType           => e
    case _ if e.dataType narrowerThan dataType => e cast dataType
    case _                                     => throw new ImplicitCastException(e, dataType)
  }

  /**
   * Tries to figure out the widest type of all input [[types.DataType DataType]]s.  For two types
   * `x` and `y`, `x` is considered to be wider than `y` iff `y` is [[implicitlyConvertible]] to
   * `x`.
   */
  def widestTypeOf(types: Seq[DataType]): Try[DataType] = (types.tail foldLeft Try(types.head)) {
    case (Success(x), y) => x widest y
    case (failure, _)    => failure
  }
}
