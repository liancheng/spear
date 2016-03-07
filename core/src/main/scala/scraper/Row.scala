package scraper

import scala.collection.mutable.ArrayBuffer

trait Row extends Seq[Any]

object Row {
  val empty = new BasicRow(Nil)

  def apply(first: Any, rest: Any*): Row = new BasicRow(first +: rest)

  def fromSeq(values: Seq[Any]): Row = new BasicRow(values)

  def unapplySeq(row: Row): Some[Seq[Any]] = Some(row)
}

trait MutableRow extends Row {
  def update(ordinal: Int, value: Any): Unit
}

class BasicRow(values: Seq[Any]) extends Row {
  override def length: Int = values.length

  override def apply(ordinal: Int): Any = values.apply(ordinal)

  override def iterator: Iterator[Any] = values.iterator
}

class BasicMutableRow(values: ArrayBuffer[Any]) extends MutableRow {
  def this(size: Int) = this(ArrayBuffer.fill(size)(null: Any))

  override def update(ordinal: Int, value: Any): Unit = values(ordinal) = value

  override def length: Int = values.length

  override def apply(ordinal: Int): Any = values(ordinal)

  override def iterator: Iterator[Any] = values.iterator
}

class JoinedRow(row1: Row, row2: Row) extends Row {
  override def length: Int = row1.length + row2.length

  override def apply(ordinal: Int): Any =
    if (ordinal < row1.length) row1(ordinal) else row2(ordinal - row1.length)

  override def iterator: Iterator[Any] = row1.iterator ++ row2.iterator
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
