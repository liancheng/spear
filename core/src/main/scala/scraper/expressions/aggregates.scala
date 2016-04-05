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

  def accumulate(buffer: MutableRow, row: Row): Unit

  def merge(into: MutableRow, from: Row): Unit

  def result(buffer: Row): Any
}

trait DeclarativeAggregateFunction extends AggregateFunction {
  def bufferAttributes: Seq[AttributeRef] = Seq(
    'value of dataType withNullability isNullable
  )

  protected lazy val value = bufferAttributes.head at 0

  override def bufferSchema: StructType = StructType fromAttributes bufferAttributes

  def zeroExpressions: Seq[Expression] = bufferSchema.fieldTypes map (lit(null) cast _)

  def accumulateExpressions: Seq[Expression]

  def mergeExpressions: Seq[Expression]

  def resultExpression: Expression = bufferAttributes.head at 0

  override def zero(buffer: MutableRow): Unit = buffer.indices foreach { i =>
    buffer(i) = zeroExpressions(i).evaluated
  }

  private lazy val accumulator = updateWith(accumulateExpressions) _

  override def accumulate(buffer: MutableRow, row: Row): Unit = accumulator(buffer, row)

  private lazy val merger = updateWith(mergeExpressions) _

  override def merge(into: MutableRow, from: Row): Unit = merger(into, from)

  override def result(buffer: Row): Any = resultExpression evaluate buffer

  private lazy val joinedRow = new JoinedRow()

  private def updateWith(expressions: Seq[Expression])(buffer: MutableRow, row: Row): Unit = {
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

  def accumulateFunction: (Expression, Expression) => Expression

  def mergeFunction: (Expression, Expression) => Expression = accumulateFunction

  override def zeroExpressions: Seq[Expression] = Seq(lit(null) cast dataType)

  override def accumulateExpressions: Seq[Expression] = Seq(
    If(
      child.isNull,
      buffered(value),
      If(
        buffered(value).isNull,
        child,
        accumulateFunction(child, buffered(value))
      )
    )
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    mergeFunction(value, buffered(value))
  )
}

case class Count(child: Expression) extends UnaryExpression with DeclarativeAggregateFunction {
  override def dataType: DataType = LongType

  override def isNullable: Boolean = false

  override def zeroExpressions: Seq[Expression] = Seq(0L)

  override def mergeExpressions: Seq[Expression] = Seq(
    value + buffered(value)
  )

  override def accumulateExpressions: Seq[Expression] = Seq(
    If(child.isNull, buffered(value), buffered(value) + 1L)
  )
}

case class Sum(child: Expression) extends SimpleAggregateFunction {
  override def accumulateFunction: (Expression, Expression) => Expression = Plus
}

case class Max(child: Expression) extends SimpleAggregateFunction {
  override def accumulateFunction: (Expression, Expression) => Expression =
    (buffered, input) => If(input > buffered, input, buffered)
}

case class Min(child: Expression) extends SimpleAggregateFunction {
  override def accumulateFunction: (Expression, Expression) => Expression =
    (buffered, input) => If(input < buffered, input, buffered)
}

case class BoolAnd(child: Expression) extends SimpleAggregateFunction {
  override def accumulateFunction: (Expression, Expression) => Expression = And
}

case class BoolOr(child: Expression) extends SimpleAggregateFunction {
  override def accumulateFunction: (Expression, Expression) => Expression = Or
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
