package scraper.types

import scraper.expressions.Expression

trait NumericType extends PrimitiveType {
  val numeric: Numeric[InternalType]
}

object NumericType {
  def unapply(e: Expression): Option[Expression] = e.dataType match {
    case _: NumericType => Some(e)
    case _              => None
  }
}

trait IntegralType extends NumericType {
  val integral: Integral[InternalType]
}

case object ByteType extends IntegralType {
  override type InternalType = Byte
  override val integral: Integral[Byte] = implicitly[Integral[Byte]]
  override val numeric: Numeric[Byte] = implicitly[Numeric[Byte]]
  override val ordering: Ordering[Byte] = implicitly[Ordering[Byte]]
}

case object ShortType extends IntegralType {
  override type InternalType = Short
  override val integral: Integral[Short] = implicitly[Integral[Short]]
  override val numeric: Numeric[Short] = implicitly[Numeric[Short]]
  override val ordering: Ordering[Short] = implicitly[Ordering[Short]]
}

case object IntType extends IntegralType {
  override type InternalType = Int
  override val integral: Integral[Int] = implicitly[Integral[Int]]
  override val numeric: Numeric[Int] = implicitly[Numeric[Int]]
  override val ordering: Ordering[Int] = implicitly[Ordering[Int]]
}

case object LongType extends IntegralType {
  override type InternalType = Long
  override val integral: Integral[Long] = implicitly[Integral[Long]]
  override val numeric: Numeric[Long] = implicitly[Numeric[Long]]
  override val ordering: Ordering[Long] = implicitly[Ordering[Long]]
}

trait FractionalType extends NumericType {
  val fractional: Fractional[InternalType]
}

case object FloatType extends FractionalType {
  override type InternalType = Float
  override val fractional: Fractional[Float] = implicitly[Fractional[Float]]
  override val numeric: Numeric[Float] = implicitly[Numeric[Float]]
  override val ordering: Ordering[Float] = implicitly[Ordering[Float]]
}

case object DoubleType extends FractionalType {
  override type InternalType = Double
  override val fractional: Fractional[Double] = implicitly[Fractional[Double]]
  override val numeric: Numeric[Double] = implicitly[Numeric[Double]]
  override val ordering: Ordering[Double] = implicitly[Ordering[Double]]
}
