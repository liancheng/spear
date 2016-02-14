package scraper.expressions

import scala.util.Try

import scraper.types._
import scraper.{MutableRow, Row}

trait AggregateFunction extends Expression {
  def accumulatorSchema: StructType

  def zero(accumulator: MutableRow): Unit

  def partial(accumulator: MutableRow, rows: Iterator[Row]): Unit

  def merge(accumulator: MutableRow, rows: Iterator[Row]): Unit =
    partial(accumulator, rows)

  def result(accumulator: Row): Any
}

case class Count(child: Expression) extends UnaryExpression with AggregateFunction {
  override def dataType: DataType = LongType

  override def accumulatorSchema: StructType = StructType('acc -> LongType.!)

  override lazy val strictlyTypedForm: Try[Expression] = for {
    strictChild <- child.strictlyTypedForm
  } yield if (strictChild sameOrEqual child) this else copy(child = strictChild)

  override def zero(row: MutableRow): Unit = row(0) = 0L

  override def partial(accumulator: MutableRow, rows: Iterator[Row]): Unit = {
    accumulator(0) = rows.length.toLong
  }

  override def merge(accumulator: MutableRow, rows: Iterator[Row]): Unit = {
    val initial = accumulator.head.asInstanceOf[Long]
    accumulator(0) = initial + rows.map(_.head.asInstanceOf[Long]).sum
  }

  override def result(accumulator: Row): Any = accumulator.head
}
