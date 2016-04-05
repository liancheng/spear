package scraper.expressions

import scala.util.Try

import scraper.{JoinedRow, MutableRow, Row}
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.types._
import scraper.utils.trySequence

trait AggregateFunction extends Expression with UnevaluableExpression {
  def bufferSchema: StructType

  def supportPartialAggregation: Boolean = true

  def zero(buffer: MutableRow): Unit

  def update(buffer: MutableRow, row: Row): Unit

  def merge(into: MutableRow, from: Row): Unit

  def result(buffer: Row): Any
}

trait DeclarativeAggregateFunction extends AggregateFunction {
  def zeroValues: Seq[Expression] = bufferSchema.fieldTypes map (lit(null) cast _)

  def updateExpressions: Seq[Expression]

  def mergeExpressions: Seq[Expression]

  def resultExpression: Expression

  override def zero(buffer: MutableRow): Unit = buffer.indices foreach { i =>
    buffer(i) = zeroValues(i).evaluated
  }

  private lazy val updater = assignWith(updateExpressions) _

  override def update(buffer: MutableRow, row: Row): Unit = updater(buffer, row)

  private lazy val merger = assignWith(mergeExpressions) _

  override def merge(into: MutableRow, from: Row): Unit = merger(into, from)

  override def result(buffer: Row): Any = resultExpression evaluate buffer

  private lazy val joinedRow = new JoinedRow()

  private def assignWith(expressions: Seq[Expression])(buffer: MutableRow, row: Row): Unit = {
    val input = joinedRow(row, buffer)
    buffer.indices foreach { i =>
      buffer(i) = expressions(i).evaluate(input)
    }
  }

  protected def buffered(ref: BoundRef): BoundRef = ref at (ref.ordinal + bufferSchema.length)
}

trait SimpleAggregateFunction extends UnaryExpression with DeclarativeAggregateFunction {
  override def dataType: DataType = child.dataType

  override def isNullable: Boolean = child.isNullable

  override lazy val bufferSchema: StructType = StructType(
    'value -> (dataType withNullability isNullable)
  )

  protected lazy val value = bufferSchema.toAttributes.head at 0

  def updateFunction: (Expression, Expression) => Expression

  def mergeFunction: (Expression, Expression) => Expression = updateFunction

  override def zeroValues: Seq[Expression] = Seq(lit(null) cast dataType)

  override def updateExpressions: Seq[Expression] = Seq(
    coalesce(updateFunction(child, buffered(value)), buffered(value), child)
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    mergeFunction(value, buffered(value))
  )

  override def resultExpression: Expression = value
}

case class Count(child: Expression) extends UnaryExpression with DeclarativeAggregateFunction {
  override def dataType: DataType = LongType

  override def isNullable: Boolean = false

  override lazy val bufferSchema: StructType = StructType(
    'count -> dataType.!
  )

  private lazy val count = bufferSchema.toAttributes.head at 0

  override def zeroValues: Seq[Expression] = Seq(0L)

  override def updateExpressions: Seq[Expression] = Seq(
    when(child.isNull) { buffered(count) } otherwise { buffered(count) + 1L }
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    count + buffered(count)
  )

  override def resultExpression: Expression = count
}

case class Average(child: Expression) extends UnaryExpression with DeclarativeAggregateFunction {
  override def dataType: DataType = DoubleType

  override def bufferSchema: StructType = StructType(
    'sum -> (child.dataType withNullability child.isNullable),
    'count -> LongType.!
  )

  private lazy val Seq(sum, count) = (bufferSchema.toAttributes, 0 to 1).zipped map (_ at _)

  override def zeroValues: Seq[Expression] = Seq(lit(null) cast child.dataType, 0L)

  override def updateExpressions: Seq[Expression] = Seq(
    coalesce(child + buffered(sum), child, buffered(sum)),
    buffered(count) + (when(child.isNull) { 0L } otherwise { 1L })
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    coalesce(sum + buffered(sum), sum, buffered(sum)),
    coalesce(count + buffered(count), count, buffered(count))
  )

  override def resultExpression: Expression = when(count =:= 0L) {
    lit(null)
  } otherwise {
    sum / (count cast sum.dataType)
  }
}

case class Sum(child: Expression) extends SimpleAggregateFunction {
  override def updateFunction: (Expression, Expression) => Expression = Plus
}

case class Max(child: Expression) extends SimpleAggregateFunction {
  override def updateFunction: (Expression, Expression) => Expression = Greatest(_, _)
}

case class Min(child: Expression) extends SimpleAggregateFunction {
  override def updateFunction: (Expression, Expression) => Expression = Least(_, _)
}

case class BoolAnd(child: Expression) extends SimpleAggregateFunction {
  override def updateFunction: (Expression, Expression) => Expression = And
}

case class BoolOr(child: Expression) extends SimpleAggregateFunction {
  override def updateFunction: (Expression, Expression) => Expression = Or
}

case class DistinctAggregateFunction(child: AggregateFunction)
  extends AggregateFunction with UnaryExpression {

  override def dataType: DataType = child.dataType

  override def bufferSchema: StructType = child.bufferSchema

  override def result(buffer: Row): Any = child.result(buffer)

  override def supportPartialAggregation: Boolean = child.supportPartialAggregation

  override def merge(into: MutableRow, from: Row): Unit = child.merge(into, from)

  override def update(buffer: MutableRow, row: Row): Unit = child.update(buffer, row)

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
