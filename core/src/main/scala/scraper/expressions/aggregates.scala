package scraper.expressions

import scala.util.Try

import scraper.types._
import scraper.{MutableRow, Row}

trait AggregateFunction extends Expression {
  def accumulatorSchema: StructType

  def zero(accumulator: MutableRow): Unit

  def accumulate(accumulator: MutableRow, row: Row): Unit

  def merge(into: MutableRow, from: Row): Unit

  def result(accumulator: Row): Any
}

// TODO How to handle NULL?
case class Count(child: Expression) extends UnaryExpression with AggregateFunction {
  override def dataType: DataType = LongType

  override def accumulatorSchema: StructType = StructType('acc -> LongType.!)

  override lazy val strictlyTypedForm: Try[Expression] = for {
    strictChild <- child.strictlyTypedForm
  } yield if (strictChild sameOrEqual child) this else copy(child = strictChild)

  override def zero(row: MutableRow): Unit = row(0) = 0L

  override def accumulate(accumulator: MutableRow, row: Row): Unit = {
    val current = accumulator.head.asInstanceOf[Long]
    accumulator(0) = current + 1L
  }

  override def merge(into: MutableRow, from: Row): Unit = {
    val current = into.head.asInstanceOf[Long]
    into(0) = current + from.head.asInstanceOf[Long]
  }

  override def result(accumulator: Row): Any = accumulator.head
}
