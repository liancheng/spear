package scraper.expressions

import scala.util.{Success, Try}

import scraper.Row
import scraper.exceptions.{ImplicitCastException, TypeCastException}
import scraper.expressions.Cast.{buildCast, castable}
import scraper.types._

case class Cast(child: Expression, override val dataType: DataType) extends UnaryExpression {
  override lazy val strictlyTyped: Try[Expression] = child.strictlyTyped map {
    case e if e.dataType == dataType         => this
    case e if castable(e.dataType, dataType) => copy(child = e)
    case e                                   => throw new TypeCastException(e.dataType, dataType)
  }

  override protected def template(childString: String): String =
    s"CAST($childString AS ${dataType.sql})"

  private def fromType = child.dataType

  override def nullSafeEvaluate(value: Any): Any =
    if (fromType == dataType) value else buildCast(fromType, dataType).get(value)
}

object Cast {
  private type PartialCastBuilder = PartialFunction[DataType, Any => Any]

  private type CastBuilder = PartialFunction[DataType, PartialCastBuilder]

  private type DataTypeComparator = (DataType, DataType) => Boolean

  private val implicitlyFromBoolean: PartialCastBuilder = {
    case StringType => _.toString
  }

  private val asBoolean = (_: Any) match { case v: Boolean => v }

  private val explicitlyFromBoolean: PartialCastBuilder = {
    case IntType => asBoolean andThen (if (_) 1 else 0)
  }

  private val asByte = (_: Any) match { case v: Byte => v }

  private val implicitlyFromByte: PartialCastBuilder = {
    case ShortType  => asByte andThen { _.toShort }
    case IntType    => asByte andThen { _.toInt }
    case LongType   => asByte andThen { _.toLong }
    case FloatType  => asByte andThen { _.toFloat }
    case DoubleType => asByte andThen { _.toDouble }
    case StringType => _.toString
  }

  private val explicitlyFromByte: PartialCastBuilder = {
    case _ if false => identity
  }

  private val asShort = (_: Any) match { case v: Short => v }

  private val implicitlyFromShort: PartialCastBuilder = {
    case IntType    => asShort andThen { _.toInt }
    case LongType   => asShort andThen { _.toLong }
    case FloatType  => asShort andThen { _.toFloat }
    case DoubleType => asShort andThen { _.toDouble }
    case StringType => _.toString
  }

  private val explicitlyFromShort: PartialCastBuilder = {
    case ByteType => asShort andThen { _.toByte }
  }

  private val asInt = (_: Any) match { case v: Int => v }

  private val implicitlyFromInt: PartialCastBuilder = {
    case BooleanType => asInt andThen { _ != 0 }
    case LongType    => asInt andThen { _.toLong }
    case FloatType   => asInt andThen { _.toFloat }
    case DoubleType  => asInt andThen { _.toDouble }
    case StringType  => _.toString
  }

  private val explicitlyFromInt: PartialCastBuilder = {
    case ByteType  => asInt andThen { _.toByte }
    case ShortType => asInt andThen { _.toShort }
  }

  private val asLong = (_: Any) match { case v: Long => v }

  private val implicitlyFromLong: PartialCastBuilder = {
    case FloatType  => asLong andThen { _.toFloat }
    case DoubleType => asLong andThen { _.toDouble }
    case StringType => _.toString
  }

  private val explicitlyFromLong: PartialCastBuilder = {
    case ByteType  => asLong andThen { _.toByte }
    case ShortType => asLong andThen { _.toShort }
    case IntType   => asLong andThen { _.toInt }
  }

  private val asFloat = (_: Any) match { case v: Float => v }

  private val implicitlyFromFloat: PartialCastBuilder = {
    case DoubleType => asFloat andThen { _.toDouble }
    case StringType => _.toString
  }

  private val explicitlyFromFloat: PartialCastBuilder = {
    case ByteType  => asFloat andThen { _.toByte }
    case ShortType => asFloat andThen { _.toShort }
    case IntType   => asFloat andThen { _.toInt }
    case LongType  => asFloat andThen { _.toLong }
  }

  private val implicitlyFromDouble: PartialCastBuilder = {
    case StringType => _.toString
  }

  private val asDouble = (_: Any) match { case v: Double => v }

  private val explicitlyFromDouble: PartialCastBuilder = {
    case ByteType  => asDouble andThen { _.toByte }
    case ShortType => asDouble andThen { _.toShort }
    case IntType   => asDouble andThen { _.toInt }
    case LongType  => asDouble andThen { _.toLong }
    case FloatType => asDouble andThen { _.toFloat }
  }

  private[scraper] val booleanTrueStrings = Set("yes", "y", "true", "t", "on")

  private[scraper] val booleanFalseStrings = Set("no", "n", "false", "f", "off")

  private[scraper] val booleanStrings = booleanTrueStrings ++ booleanFalseStrings

  private val asString = (_: Any) match { case v: String => v }

  private def stringToBoolean(value: String): Boolean = value match {
    case _ if booleanTrueStrings contains value  => true
    case _ if booleanFalseStrings contains value => false
    case _ =>
      throw new TypeCastException(s"Can't cast string [$value] to boolean")
  }

  private val implicitlyFromString: PartialCastBuilder = {
    case BooleanType => asString andThen { _.toLowerCase } andThen stringToBoolean
  }

  private val explicitlyFromString: PartialCastBuilder = {
    case ByteType   => asString andThen { _.toByte }
    case ShortType  => asString andThen { _.toShort }
    case IntType    => asString andThen { _.toInt }
    case LongType   => asString andThen { _.toLong }
    case FloatType  => asString andThen { _.toFloat }
    case DoubleType => asString andThen { _.toDouble }
  }

  private val implicitlyFromNull: PartialCastBuilder = {
    // NullType can be casted to any other type, and only has a single value `null`.
    case _ => _ => null
  }

  private val explicitlyFromNull: PartialCastBuilder = {
    case _ if false => identity
  }

  private val asSeq = (_: Any) match { case v: Seq[_] => v }

  private def compareArrayType(x: ArrayType, y: ArrayType, fn: DataTypeComparator): Boolean = {
    fn(x.elementType, y.elementType) && (!x.isElementNullable || y.isElementNullable)
  }

  private def implicitlyFromArrayType(from: ArrayType): PartialCastBuilder =
    castFromArrayType(from, compatible)

  private def explicitlyFromArrayType(from: ArrayType): PartialCastBuilder =
    castFromArrayType(from, castable)

  private def castFromArrayType(from: ArrayType, fn: DataTypeComparator): PartialCastBuilder = {
    case to: ArrayType if compareArrayType(from, to, fn) =>
      val castElement = buildCast(to.elementType, from.elementType).get
      asSeq andThen (_ map castElement)
  }

  private val asMap = (_: Any) match { case v: Map[_, _] => v }

  private def compareMapType(x: MapType, y: MapType, fn: DataTypeComparator): Boolean = {
    (!x.isValueNullable || y.isValueNullable) &&
      fn(x.keyType, y.keyType) &&
      fn(x.valueType, y.valueType)
  }

  private def implicitlyFromMapType(from: MapType): PartialCastBuilder =
    castFromMapType(from, compatible)

  private def explicitlyFromMapType(from: MapType): PartialCastBuilder =
    castFromMapType(from, castable)

  private def castFromMapType(from: MapType, fn: DataTypeComparator): PartialCastBuilder = {
    case to: MapType if compareMapType(from, to, fn) =>
      val castKey = buildCast(from.keyType, to.keyType).get
      val castValue = buildCast(from.valueType, to.valueType).get
      asMap andThen (_ map { case (key, value) => castKey(key) -> castValue(value) })
  }

  private def compareStructType(x: StructType, y: StructType, fn: DataTypeComparator): Boolean = {
    x.fields.length == y.fields.length && (x.fields zip y.fields).forall {
      case (from, to) =>
        (from.name == to.name) &&
          (!from.isNullable || to.isNullable) &&
          fn(from.dataType, to.dataType)
    }
  }

  private val asRow = (_: Any) match { case v: Row => v }

  private def implicitlyFromStructType(from: StructType): PartialCastBuilder =
    castFromStructType(from, compatible)

  private def explicitlyFromStructType(from: StructType): PartialCastBuilder =
    castFromStructType(from, castable)

  private def castFromStructType(from: StructType, fn: DataTypeComparator): PartialCastBuilder = {
    case to: StructType if compareStructType(from, to, fn) =>
      val castFields = (from.fieldTypes, to.fieldTypes).zipped map (buildCast(_, _).get)
      asRow andThen { row =>
        Row.fromSeq(row zip castFields map { case (value, cast) => cast(value) })
      }
  }

  private val buildImplicitCast: CastBuilder = {
    case BooleanType   => implicitlyFromBoolean
    case ByteType      => implicitlyFromByte
    case ShortType     => implicitlyFromShort
    case IntType       => implicitlyFromInt
    case LongType      => implicitlyFromLong
    case FloatType     => implicitlyFromFloat
    case DoubleType    => implicitlyFromDouble
    case StringType    => implicitlyFromString
    case NullType      => implicitlyFromNull
    case t: ArrayType  => implicitlyFromArrayType(t)
    case t: MapType    => implicitlyFromMapType(t)
    case t: StructType => implicitlyFromStructType(t)
  }

  private val buildExplicitCast: CastBuilder = {
    case BooleanType   => explicitlyFromBoolean
    case ByteType      => explicitlyFromByte
    case ShortType     => explicitlyFromShort
    case IntType       => explicitlyFromInt
    case LongType      => explicitlyFromLong
    case FloatType     => explicitlyFromFloat
    case DoubleType    => explicitlyFromDouble
    case StringType    => explicitlyFromString
    case NullType      => explicitlyFromNull
    case t: ArrayType  => explicitlyFromArrayType(t)
    case t: MapType    => explicitlyFromMapType(t)
    case t: StructType => explicitlyFromStructType(t)
  }

  private def buildCast(x: DataType, y: DataType): Option[Any => Any] =
    buildImplicitCast lift x flatMap (_ lift y) orElse {
      buildExplicitCast lift x flatMap (_ lift y)
    }

  /**
   * Returns true iff `x` equals to `y` or `x` can be implicitly casted to `y`.
   */
  def compatible(x: DataType, y: DataType): Boolean =
    x == y || buildImplicitCast.lift(x).exists(_ isDefinedAt y)

  /**
   * Returns true iff `x` equals to `y` or `x` can be casted to `y`, either implicitly or
   * explicitly.
   */
  def castable(x: DataType, y: DataType): Boolean = x == y || buildCast(x, y).isDefined

  /**
   * Returns a new [[Expression]] that casts [[Expression]] `e` to `dataType` if the data type of
   * `e` is [[compatible]] to `dataType`.  If `e` is already of the target type, it is returned
   * untouched.
   */
  def widenDataTypeTo(dataType: DataType)(e: Expression): Expression = e match {
    case _ if e.dataType == dataType               => e
    case _ if e.dataType isCompatibleWith dataType => e cast dataType
    case _                                         => throw new ImplicitCastException(e, dataType)
  }

  /**
   * Tries to figure out the widest type of all input data types.  For two types `x` and `y`, `x` is
   * considered to be wider than `y` iff `y` is [[compatible]] to `x`.
   */
  def widestTypeOf(types: Seq[DataType]): Try[DataType] = (types.tail foldLeft Try(types.head)) {
    case (Success(x), y) => x widest y
    case (failure, _)    => failure
  }

  def printCastingTable(): Unit = {
    val numericTypes = Seq(ByteType, ShortType, IntType, LongType, FloatType, DoubleType)
    val primitiveTypes = Seq(NullType, BooleanType, StringType) ++ numericTypes

    val header = ("from \\ to" +: primitiveTypes.map(_.sql)).map(t => f"$t%10s").mkString
    val body = for (from <- primitiveTypes) yield {
      val marks = for (to <- primitiveTypes) yield from -> to match {
        case _ if from isCompatibleWith to => "*"
        case _ if from isCastableTo to     => "o"
        case _                             => "_"
      }

      (from.sql +: marks).map(s => f"$s%10s").mkString
    }

    header +: body foreach println

    println()
    println("*: Implicitly castable")
    println("o: Explicitly castable")
    println("_: Not castable")
  }
}
