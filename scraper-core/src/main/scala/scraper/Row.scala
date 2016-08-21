package scraper

import scala.collection.mutable.ArrayBuffer

trait SpecializedGetter {
  def getBoolean(ordinal: Int): Boolean

  def getByte(ordinal: Int): Byte

  def getShort(ordinal: Int): Short

  def getInt(ordinal: Int): Int

  def getLong(ordinal: Int): Long

  def getFloat(ordinal: Int): Float

  def getDouble(ordinal: Int): Double
}

trait SpecializedSetter {
  def setBoolean(ordinal: Int, value: Boolean): Unit

  def setByte(ordinal: Int, value: Byte): Unit

  def setShort(ordinal: Int, value: Short): Unit

  def setInt(ordinal: Int, value: Int): Unit

  def setLong(ordinal: Int, value: Long): Unit

  def setFloat(ordinal: Int, value: Float): Unit

  def setDouble(ordinal: Int, value: Double): Unit
}

trait Row extends Seq[Any] with SpecializedGetter {
  override def getBoolean(ordinal: Int): Boolean = apply(ordinal).asInstanceOf[Boolean]

  override def getDouble(ordinal: Int): Double = apply(ordinal).asInstanceOf[Double]

  override def getFloat(ordinal: Int): Float = apply(ordinal).asInstanceOf[Float]

  override def getLong(ordinal: Int): Long = apply(ordinal).asInstanceOf[Long]

  override def getByte(ordinal: Int): Byte = apply(ordinal).asInstanceOf[Byte]

  override def getShort(ordinal: Int): Short = apply(ordinal).asInstanceOf[Short]

  override def getInt(ordinal: Int): Int = apply(ordinal).asInstanceOf[Int]
}

object Row {
  val empty = new BasicRow(Nil)

  def apply(first: Any, rest: Any*): Row = new BasicRow(first +: rest)

  def fromSeq(values: Seq[Any]): Row = new BasicRow(values)

  def unapplySeq(row: Row): Some[Seq[Any]] = Some(row)
}

trait MutableRow extends Row with SpecializedSetter {
  def update(ordinal: Int, value: Any): Unit

  def setBoolean(ordinal: Int, value: Boolean): Unit = update(ordinal, value)

  def setByte(ordinal: Int, value: Byte): Unit = update(ordinal, value)

  def setShort(ordinal: Int, value: Short): Unit = update(ordinal, value)

  def setInt(ordinal: Int, value: Int): Unit = update(ordinal, value)

  def setLong(ordinal: Int, value: Long): Unit = update(ordinal, value)

  def setFloat(ordinal: Int, value: Float): Unit = update(ordinal, value)

  def setDouble(ordinal: Int, value: Double): Unit = update(ordinal, value)
}

class BasicRow(values: Seq[Any]) extends Row {
  override def length: Int = values.length

  override def apply(ordinal: Int): Any = values.apply(ordinal)

  override def iterator: Iterator[Any] = values.iterator
}

class BasicMutableRow(values: ArrayBuffer[Any]) extends BasicRow(values) with MutableRow {
  def this(size: Int) = this(ArrayBuffer.fill(size)(null: Any))

  override def update(ordinal: Int, value: Any): Unit = values(ordinal) = value

  override def length: Int = values.length

  override def apply(ordinal: Int): Any = values(ordinal)

  override def iterator: Iterator[Any] = values.iterator
}

class JoinedRow(private var left: Row, private var right: Row) extends Row {
  def this() = this(null, null)

  override def length: Int = left.length + right.length

  override def apply(ordinal: Int): Any =
    if (ordinal < left.length) left(ordinal) else right(ordinal - left.length)

  override def iterator: Iterator[Any] = left.iterator ++ right.iterator

  def apply(lhs: Row, rhs: Row): this.type = {
    left = lhs
    right = rhs
    this
  }
}

class RowSlice(row: Row, begin: Int, override val length: Int) extends Row {
  require(begin >= 0 && length >= 0 && begin + length <= row.length)

  override def apply(ordinal: Int): Any = {
    require(ordinal < length)
    row(begin + ordinal)
  }

  override def iterator: Iterator[Any] = row.iterator.slice(begin, begin + length)
}

class MutableRowSlice(row: MutableRow, begin: Int, length: Int)
  extends RowSlice(row, begin, length) with MutableRow {

  def update(ordinal: Int, value: Any): Unit = row(begin + ordinal) = value
}
