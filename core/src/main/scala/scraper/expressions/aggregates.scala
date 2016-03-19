package scraper.expressions

import scraper.types._
import scraper.{MutableRow, Row}

trait AggregateFunction extends Expression {
  override def isFoldable: Boolean = false

  def bufferSchema: StructType

  def zero(buffer: MutableRow): Unit

  def accumulate(buffer: MutableRow, row: Row): Unit

  def merge(into: MutableRow, from: Row): Unit

  def result(buffer: Row): Any

  def asAgg: AggregationAlias = AggregationAlias(this)
}

case class Count(child: Expression) extends UnaryExpression with AggregateFunction {
  override def dataType: DataType = LongType

  override def isNullable: Boolean = false

  override def bufferSchema: StructType = StructType('count -> LongType.!)

  override def zero(row: MutableRow): Unit = row(0) = 0L

  override def accumulate(buffer: MutableRow, row: Row): Unit = {
    if (child.evaluate(row) != null) {
      val current = buffer.head.asInstanceOf[Long]
      buffer(0) = current + 1L
    }
  }

  override def merge(into: MutableRow, from: Row): Unit = {
    val current = into.head.asInstanceOf[Long]
    into(0) = current + from.head.asInstanceOf[Long]
  }

  override def result(buffer: Row): Any = buffer.head
}
