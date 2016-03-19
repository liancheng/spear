package scraper.expressions

import scraper.types._
import scraper.{MutableRow, Row}

trait AggregateFunction extends Expression {
  override def isFoldable: Boolean = false

  def bufferSchema: StructType

  def supportPartialAggregation: Boolean

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

  override def supportPartialAggregation: Boolean = true

  override def zero(buffer: MutableRow): Unit = buffer.setLong(0, 0L)

  override def accumulate(buffer: MutableRow, row: Row): Unit = if (child.evaluate(row) != null) {
    val current = buffer.getLong(0)
    buffer.setLong(0, current + 1L)
  }

  override def merge(into: MutableRow, from: Row): Unit =
    into.setLong(0, into.getLong(0) + from.getLong(0))

  override def result(buffer: Row): Any = buffer.getLong(0)
}
