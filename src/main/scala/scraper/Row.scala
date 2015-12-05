package scraper

trait Row extends Seq[Any]

class GenericRow(values: Seq[Any]) extends Row {
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

object Row {
  val empty = new GenericRow(Nil)

  def apply(first: Any, rest: Any*): Row = new GenericRow(first +: rest)

  def fromSeq(values: Seq[Any]): Row = new GenericRow(values)
}
