package scraper.expressions

import scala.util.Try

import scraper.{MutableRow, Row}
import scraper.types._
import scraper.utils.trySequence

trait AggregateFunction extends Expression with UnevaluableExpression {
  def bufferSchema: StructType

  def supportPartialAggregation: Boolean

  def zero(buffer: MutableRow): Unit

  def accumulate(buffer: MutableRow, row: Row): Unit

  def merge(into: MutableRow, from: Row): Unit

  def result(buffer: Row): Any
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

case class DistinctAggregateFunction(child: AggregateFunction)
  extends AggregateFunction with UnaryExpression {

  override def dataType: DataType = child.dataType

  override def bufferSchema: StructType = child.bufferSchema

  override def result(buffer: Row): Any = child.result(buffer)

  override def supportPartialAggregation: Boolean = child.supportPartialAggregation

  override def merge(into: MutableRow, from: Row): Unit = child.merge(into, from)

  override def accumulate(buffer: MutableRow, row: Row): Unit = child.accumulate(buffer, row)

  override def zero(buffer: MutableRow): Unit = child.zero(buffer)

  override def sql: Try[String] = for {
    argSQL <- trySequence(child.children.map(_.sql))
    name = child.nodeName.toUpperCase
  } yield s"$name(DISTINCT ${argSQL mkString ", "})"

  override def debugString: String = {
    val args = child.children map (_.debugString)
    val name = child.nodeName.toUpperCase
    s"$name(DISTINCT ${args mkString ", "})"
  }
}
