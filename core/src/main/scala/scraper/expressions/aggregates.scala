package scraper.expressions

import scala.util.Try

import scraper.{JoinedRow, MutableRow, Row}
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.types._
import scraper.utils.trySequence

/**
 * A trait for aggregate functions, which aggregate a group of values into a single scalar value.
 */
trait AggregateFunction extends Expression with UnevaluableExpression {
  def bufferSchema: StructType

  def supportPartialAggregation: Boolean = true

  /**
   * Initializes the aggregate buffer with zero value(s).
   */
  def zero(aggBuffer: MutableRow): Unit

  def update(input: Row, aggBuffer: MutableRow): Unit

  def merge(fromAggBuffer: Row, toAggBuffer: MutableRow): Unit

  def result(resultBuffer: MutableRow, ordinal: Int, aggBuffer: Row): Unit
}

trait DeclarativeAggregateFunction extends AggregateFunction {
  def zeroValues: Seq[Expression]

  def updateExpressions: Seq[Expression]

  def mergeExpressions: Seq[Expression]

  def resultExpression: Expression

  override def zero(aggBuffer: MutableRow): Unit = aggBuffer.indices foreach { i =>
    aggBuffer(i) = zeroValues(i).evaluated
  }

  override def update(input: Row, aggBuffer: MutableRow): Unit = updater(aggBuffer, input)

  override def merge(fromAggBuffer: Row, toAggBuffer: MutableRow): Unit =
    merger(toAggBuffer, fromAggBuffer)

  override def result(resultBuffer: MutableRow, ordinal: Int, aggBuffer: Row): Unit =
    resultBuffer(ordinal) = resultExpression evaluate aggBuffer

  protected lazy val reboundChildren = children map rebind

  private lazy val updater = inPlaceUpdate(updateExpressions) _

  private lazy val merger = inPlaceUpdate(mergeExpressions) _

  private lazy val joinedRow = new JoinedRow()

  protected def rebind(expression: Expression): Expression = expression transformDown {
    case ref: BoundRef => ref at (ref.ordinal + bufferSchema.length)
  }

  /**
   * Updates mutable row `left` in-place using `expressions` and row `right` by:
   *
   *  1. Join `left` and `right` into a single [[JoinedRow]];
   *  2. Use the [[JoinedRow]] as input row to evaluate each given `expressions` to produce `n`
   *     result values, where `n` is the length of both `left` and `expression`;
   *  3. Update the i-th cell of `left` using the i-th evaluated result value.
   */
  private def inPlaceUpdate(expressions: Seq[Expression])(left: MutableRow, right: Row): Unit = {
    require(expressions.length == left.length)

    val input = joinedRow(left, right)
    left.indices foreach { i =>
      left(i) = expressions(i) evaluate input
    }
  }
}

trait UnaryDeclarativeAggregateFunction extends UnaryExpression with DeclarativeAggregateFunction {
  protected lazy val reboundChild = reboundChildren.head
}

abstract class ReduceLike(updateFunction: (Expression, Expression) => Expression)
  extends UnaryDeclarativeAggregateFunction {

  override def dataType: DataType = child.dataType

  override def isNullable: Boolean = child.isNullable

  override lazy val bufferSchema: StructType = StructType(
    'value -> (dataType withNullability isNullable)
  )

  protected lazy val value = bufferSchema.toAttributes.head at 0

  override def zeroValues: Seq[Expression] = Seq(lit(null) cast dataType)

  override def updateExpressions: Seq[Expression] = Seq(
    coalesce(updateFunction(reboundChild, value), value, reboundChild)
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    updateFunction(value, rebind(value))
  )

  override def resultExpression: Expression = value
}

case class Count(child: Expression) extends UnaryDeclarativeAggregateFunction {
  override def dataType: DataType = LongType

  override def isNullable: Boolean = false

  override lazy val bufferSchema: StructType = StructType(
    'count -> dataType.!
  )

  private lazy val count = bufferSchema.toAttributes.head at 0

  override def zeroValues: Seq[Expression] = Seq(0L)

  override def updateExpressions: Seq[Expression] = Seq(
    If(reboundChild.isNull, count, count + 1L)
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    count + rebind(count)
  )

  override def resultExpression: Expression = count
}

case class Average(child: Expression) extends UnaryDeclarativeAggregateFunction {
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

  override def resultExpression: Expression =
    If(count =:= 0L, lit(null), sum / (count cast dataType))
}

abstract class FirstLike(child: Expression) extends UnaryDeclarativeAggregateFunction {
  override def dataType: DataType = child.dataType

  override def isNullable: Boolean = child.isNullable

  override def bufferSchema: StructType = StructType(
    'value -> (dataType withNullability isNullable)
  )

  protected lazy val value: BoundRef = bufferSchema.toAttributes.head at 0

  override def zeroValues: Seq[Expression] = Seq(lit(null) cast dataType)

  override def resultExpression: Expression = value
}

case class First(child: Expression) extends FirstLike(child) {
  override def updateExpressions: Seq[Expression] = Seq(
    coalesce(value, reboundChild)
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    coalesce(value, rebind(value))
  )
}

case class Last(child: Expression) extends FirstLike(child) {
  override def updateExpressions: Seq[Expression] = Seq(
    coalesce(reboundChild, value)
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    coalesce(rebind(value), value)
  )
}

case class Sum(child: Expression) extends ReduceLike(Plus)

case class Max(child: Expression) extends ReduceLike(Greatest(_, _))

case class Min(child: Expression) extends ReduceLike(Least(_, _))

case class BoolAnd(child: Expression) extends ReduceLike(And)

case class BoolOr(child: Expression) extends ReduceLike(Or)

case class DistinctAggregateFunction(child: AggregateFunction)
  extends AggregateFunction with UnaryExpression {

  override def dataType: DataType = child.dataType

  override def bufferSchema: StructType = child.bufferSchema

  override def supportPartialAggregation: Boolean = child.supportPartialAggregation

  override def zero(aggBuffer: MutableRow): Unit = child.zero(aggBuffer)

  override def update(input: Row, aggBuffer: MutableRow): Unit = child.update(input, aggBuffer)

  override def merge(fromAggBuffer: Row, intoAggBuffer: MutableRow): Unit =
    child.merge(fromAggBuffer, intoAggBuffer)

  override def result(into: MutableRow, ordinal: Int, from: Row): Unit =
    child.result(into, ordinal, from)

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
