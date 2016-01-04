package scraper

import scala.collection.mutable

trait Row extends Seq[Any]

trait MutableRow extends Row {
  def update(ordinal: Int, value: Any): Unit
}

class BasicRow(values: Seq[Any]) extends Row {
  override def length: Int = values.length

  override def apply(index: Int): Any = values.apply(index)

  override def iterator: Iterator[Any] = values.iterator
}

case class JoinedRow(row1: Row, row2: Row) extends Row {
  override def length: Int = row1.length + row2.length

  override def apply(idx: Int): Any =
    if (idx < row1.length) row1(idx) else row2(idx - row1.length)

  override def iterator: Iterator[Any] = row1.iterator ++ row2.iterator
}

class BasicMutableRow(values: mutable.ArrayBuffer[Any]) extends MutableRow {
  def this(size: Int) = this(mutable.ArrayBuffer.fill(size)(null: Any))

  override def update(ordinal: Int, value: Any): Unit = values(ordinal) = value

  override def length: Int = values.length

  override def apply(index: Int): Any = values(index)

  override def iterator: Iterator[Any] = values.iterator
}

object Row {
  val empty = new BasicRow(Nil)

  def apply(first: Any, rest: Any*): Row = new BasicRow(first +: rest)

  def fromSeq(values: Seq[Any]): Row = new BasicRow(values)

  def unapplySeq(row: Row): Some[Seq[Any]] = Some(row)
}
