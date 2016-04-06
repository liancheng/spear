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
    val input = joinedRow(buffer, row)
    buffer.indices foreach { i =>
      buffer(i) = expressions(i) evaluate input
    }
  }

  protected lazy val reboundChildren = children map rebind

  protected def rebind(expression: Expression): Expression = expression transformDown {
    case ref: BoundRef => ref at (ref.ordinal + bufferSchema.length)
  }
}

trait NullableMonoidAggregateFunction extends UnaryExpression with DeclarativeAggregateFunction {
  protected type BinaryFunction = (Expression, Expression) => Expression

  override def dataType: DataType = child.dataType

  override def isNullable: Boolean = child.isNullable

  override lazy val bufferSchema: StructType = StructType(
    'value -> (dataType withNullability isNullable)
  )

  protected lazy val value = bufferSchema.toAttributes.head at 0

  protected lazy val reboundChild = reboundChildren.head

  def updateFunction: BinaryFunction

  def mergeFunction: BinaryFunction = updateFunction

  override def zeroValues: Seq[Expression] = Seq(lit(null) cast dataType)

  override def updateExpressions: Seq[Expression] = Seq(
    coalesce(updateFunction(reboundChild, value), value, reboundChild)
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    mergeFunction(value, rebind(value))
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

  private lazy val reboundChild = reboundChildren.head

  override def zeroValues: Seq[Expression] = Seq(0L)

  override def updateExpressions: Seq[Expression] = Seq(
    If(reboundChild.isNull, count, count + 1L)
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    count + rebind(count)
  )

  override def resultExpression: Expression = count
}

case class Average(child: Expression) extends UnaryExpression with DeclarativeAggregateFunction {
  override def nodeName: String = "AVG"

  override def dataType: DataType = DoubleType

  override def bufferSchema: StructType = StructType(
    'sum -> (dataType withNullability child.isNullable),
    'count -> LongType.!
  )

  private lazy val (sum, count) = {
    val Seq(unboundSum, unboundCount) = bufferSchema.toAttributes
    (unboundSum at 0, unboundCount at 1)
  }

  private lazy val reboundChild = reboundChildren.head

  override def zeroValues: Seq[Expression] = Seq(lit(null) cast child.dataType, 0L)

  override def updateExpressions: Seq[Expression] = Seq(
    let('c -> (reboundChild cast dataType)) {
      coalesce('c + sum, 'c, sum)
    },

    count + If(reboundChild.isNull, 0L, 1L)
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    coalesce(sum + rebind(sum), sum, rebind(sum)),
    coalesce(count + rebind(count), count, rebind(count))
  )

  override def resultExpression: Expression = let('c -> count) {
    If('c =:= 0L, lit(null), sum / ('c cast dataType))
  }
}

case class Sum(child: Expression) extends NullableMonoidAggregateFunction {
  override def updateFunction: BinaryFunction = Plus
}

case class Max(child: Expression) extends NullableMonoidAggregateFunction {
  override def updateFunction: BinaryFunction = Greatest(_, _)
}

case class Min(child: Expression) extends NullableMonoidAggregateFunction {
  override def updateFunction: BinaryFunction = Least(_, _)
}

case class BoolAnd(child: Expression) extends NullableMonoidAggregateFunction {
  override def updateFunction: BinaryFunction = And
}

case class BoolOr(child: Expression) extends NullableMonoidAggregateFunction {
  override def updateFunction: BinaryFunction = Or
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
