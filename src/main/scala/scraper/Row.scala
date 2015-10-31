package scraper

class Row(values: Seq[Any]) extends Seq[Any] {
  override def length: Int = values.length

  override def apply(index: Int): Any = values.apply(index)

  override def iterator: Iterator[Any] = values.iterator
}

object Row {
  val empty = new Row(Nil)

  def apply(values: Any*): Row = new Row(values)
}
