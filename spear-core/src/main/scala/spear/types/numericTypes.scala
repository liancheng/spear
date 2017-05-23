package spear.types

trait NumericType extends PrimitiveType {
  val numeric: Numeric[InternalType]

  def genericNumeric: Numeric[Any] = numeric.asInstanceOf[Numeric[Any]]
}

object NumericType extends AbstractDataType {
  override def isSupertypeOf(dataType: DataType): Boolean = dataType match {
    case _: NumericType => true
    case _              => false
  }

  override def toString: String = "numeric type"
}

trait IntegralType extends NumericType {
  val integral: Integral[InternalType]

  def genericIntegral: Integral[Any] = integral.asInstanceOf[Integral[Any]]
}

object IntegralType extends AbstractDataType {
  override def isSupertypeOf(dataType: DataType): Boolean = dataType match {
    case _: IntegralType => true
    case _               => false
  }

  override def toString: String = "integral type"
}

case object ByteType extends IntegralType {
  override type InternalType = Byte

  override val integral: Integral[Byte] = implicitly[Integral[Byte]]

  override val numeric: Numeric[Byte] = implicitly[Numeric[Byte]]

  override val ordering: Option[Ordering[Byte]] = Some(Ordering.Byte)

  override def sql: String = "TINYINT"
}

case object ShortType extends IntegralType {
  override type InternalType = Short

  override val integral: Integral[Short] = implicitly[Integral[Short]]

  override val numeric: Numeric[Short] = implicitly[Numeric[Short]]

  override val ordering: Option[Ordering[Short]] = Some(Ordering.Short)

  override def sql: String = "SMALLINT"
}

case object IntType extends IntegralType {
  override type InternalType = Int

  override val integral: Integral[Int] = implicitly[Integral[Int]]

  override val numeric: Numeric[Int] = implicitly[Numeric[Int]]

  override val ordering: Option[Ordering[Int]] = Some(Ordering.Int)

  override def sql: String = "INT"
}

case object LongType extends IntegralType {
  override type InternalType = Long

  override val integral: Integral[Long] = implicitly[Integral[Long]]

  override val numeric: Numeric[Long] = implicitly[Numeric[Long]]

  override val ordering: Option[Ordering[Long]] = Some(Ordering.Long)

  override def sql: String = "BIGINT"
}

trait FractionalType extends NumericType {
  val fractional: Fractional[InternalType]

  def genericFractional: Fractional[Any] = fractional.asInstanceOf[Fractional[Any]]
}

object FractionalType extends AbstractDataType {
  override def isSupertypeOf(dataType: DataType): Boolean = dataType match {
    case _: FractionalType => true
    case _                 => false
  }

  override def toString: String = "fractional type"
}

case object FloatType extends FractionalType {
  override type InternalType = Float

  override val fractional: Fractional[Float] = implicitly[Fractional[Float]]

  override val numeric: Numeric[Float] = implicitly[Numeric[Float]]

  override val ordering: Option[Ordering[Float]] = Some(Ordering.Float)

  override def sql: String = "FLOAT"
}

case object DoubleType extends FractionalType {
  override type InternalType = Double

  override val fractional: Fractional[Double] = implicitly[Fractional[Double]]

  override val numeric: Numeric[Double] = implicitly[Numeric[Double]]

  override val ordering: Option[Ordering[Double]] = Some(Ordering.Double)

  override def sql: String = "DOUBLE"
}
