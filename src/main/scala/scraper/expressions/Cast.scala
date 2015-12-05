package scraper.expressions

import scala.util.{Failure, Success, Try}

import scraper.exceptions.{ImplicitCastException, TypeCastException, TypeMismatchException}
import scraper.expressions.Cast.{buildCast, convertible}
import scraper.types
import scraper.types._

case class Cast(child: Expression, dataType: DataType) extends UnaryExpression {
  override def annotatedString: String =
    s"CAST(${child.annotatedString} AS ${dataType.simpleName})"

  private def fromType = child.dataType

  override def nullSafeEvaluate(value: Any): Any =
    buildCast(fromType)(dataType)(value)

  override lazy val strictlyTypedForm: Try[Expression] = for {
    strictChild <- child.strictlyTypedForm map {
      case e if convertible(e.dataType, dataType) => e
      case e =>
        throw new TypeCastException(e.dataType, dataType)
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

  private val explicitlyFromBoolean: CastBuilder = {
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

  private val explicitlyFromByte: CastBuilder = {
    case _ if false => identity
  }

  private val asShort = (_: Any) match { case v: Short => v }

  private val implicitlyFromShort: CastBuilder = {
    case IntType    => asShort andThen (_.toInt)
    case LongType   => asShort andThen (_.toLong)
    case FloatType  => asShort andThen (_.toFloat)
    case DoubleType => asShort andThen (_.toDouble)
    case StringType => _.toString
  }

  private val explicitlyFromShort: CastBuilder = {
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

  private val explicitlyFromInt: CastBuilder = {
    case ByteType  => asInt andThen (_.toByte)
    case ShortType => asInt andThen (_.toShort)
  }

  private val asLong = (_: Any) match { case v: Long => v }

  private val implicitlyFromLong: CastBuilder = {
    case FloatType  => asLong andThen (_.toFloat)
    case DoubleType => asLong andThen (_.toDouble)
    case StringType => _.toString
  }

  private val explicitlyFromLong: CastBuilder = {
    case ByteType  => asLong andThen (_.toByte)
    case ShortType => asLong andThen (_.toShort)
    case IntType   => asLong andThen (_.toInt)
  }

  private val asFloat = (_: Any) match { case v: Float => v }

  private val implicitlyFromFloat: CastBuilder = {
    case DoubleType => asFloat andThen (_.toDouble)
    case StringType => _.toString
  }

  private val explicitlyFromFloat: CastBuilder = {
    case ByteType  => asFloat andThen (_.toByte)
    case ShortType => asFloat andThen (_.toShort)
    case IntType   => asFloat andThen (_.toInt)
    case LongType  => asFloat andThen (_.toLong)
  }

  private val implicitlyFromDouble: CastBuilder = {
    case StringType => _.toString
  }

  private val asDouble = (_: Any) match { case v: Double => v }

  private val explicitlyFromDouble: CastBuilder = {
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

  private val explicitlyFromString: CastBuilder = {
    case _ if false => identity
  }

  private val implicitlyFromNull: CastBuilder = {
    // NullType can be casted to any other type, and only has a single value `null`.
    case _ => { case _ => null }
  }

  private val explicitlyFromNull: CastBuilder = {
    case _ if false => identity
  }

  private val buildImplicitCast: PartialFunction[DataType, CastBuilder] = {
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

  private val buildExplicitCast: PartialFunction[DataType, CastBuilder] = {
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

  private val buildCast: PartialFunction[DataType, CastBuilder] =
    buildImplicitCast orElse buildExplicitCast

  /**
   * Returns whether type `x` can be converted to type `y` implicitly.
   *
   * @note Any [[types.DataType DataType]] is NOT considered to be [[implicitlyConvertible]] to
   *       itself.
   */
  def implicitlyConvertible(x: DataType, y: DataType): Boolean =
    buildImplicitCast lift x exists (_ isDefinedAt y)

  /**
   * Returns whether type `x` can be converted to type `y`, either implicitly or explicitly.
   *
   * @note Any [[types.DataType DataType]] is NOT considered to be [[convertible]] to itself.
   */
  def convertible(x: DataType, y: DataType): Boolean = buildCast lift x exists (_ isDefinedAt y)

  /**
   * Returns whether type `x` is implicitly compatible with type `y`. Type `x` is implicitly
   * compatible with type `y` iff:
   *
   *  - `x == y`, or
   *  - `x` is [[implicitlyConvertible]] to `y`
   */
  def implicitlyCompatible(x: DataType, y: DataType): Boolean =
    x == y || implicitlyConvertible(x, y) || implicitlyConvertible(y, x)

  /**
   * Returns a new [[Expression]] that [[Cast]]s [[Expression]] `e` to `dataType` if the
   * [[types.DataType DataType]] of `e` is [[implicitlyConvertible]] to `dataType`.  If `e` is
   * already of the target type, `e` is returned untouched.
   */
  def promoteDataType(e: Expression, dataType: DataType): Expression = e match {
    case _ if e.dataType == dataType => e
    case _ if implicitlyConvertible(e.dataType, dataType) => e cast dataType
    case _ => throw new ImplicitCastException(e, dataType)
  }

  /**
   * Tries to figure out the widest type of all input [[types.DataType DataType]]s.  For two types
   * `x` and `y`, `x` is considered to be wider than `y` iff `y` is [[implicitlyCompatible]] to `x`.
   */
  def widestTypeOf(first: DataType, second: DataType, rest: DataType*): Try[DataType] =
    widestTypeOf(Seq(first, second) ++ rest)

  /**
   * Tries to figure out the widest type of all input [[types.DataType DataType]]s.  For two types
   * `x` and `y`, `x` is considered to be wider than `y` iff `y` is [[implicitlyCompatible]] to `x`.
   */
  def widestTypeOf(types: Seq[DataType]): Try[DataType] = {
    assert(types.nonEmpty)
    (types.tail foldLeft Try(types.head)) {
      case (Success(x), y) if x == y                      => Success(x)
      case (Success(x), y) if implicitlyConvertible(x, y) => Success(y)
      case (Success(x), y) if implicitlyConvertible(y, x) => Success(x)
      case _ => Failure(new TypeMismatchException(
        s"Could not find common type for: ${types map (_.sql) mkString ", "}"
      ))
    }
  }
}
