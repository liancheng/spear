package scraper.expressions

import scala.util.Try

import scraper.{JoinedRow, MutableRow, Row}
import scraper.exceptions.BrokenContractException
import scraper.expressions.NamedExpression.newExpressionID
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.expressions.typecheck.TypeConstraint
import scraper.types._
import scraper.utils._

/**
 * A trait for aggregate functions, which aggregate a group of values into a single scalar value.
 * When being evaluated, an aggregation buffer, which is essentially a [[MutableRow]], is used to
 * store aggregated intermediate values. The aggregation buffer may have multiple fields. For
 * example, [[Average]] uses two fields to store sum and total count of all input values seen so
 * far.
 */
trait AggregateFunction extends Expression with UnevaluableExpression {
  /**
   * Schema of the aggregation buffer.
   */
  def aggBufferSchema: StructType

  /**
   * Whether this [[AggregateFunction]] supports partial aggregation
   */
  def supportPartialAggregation: Boolean = true

  /**
   * Initializes the aggregation buffer with zero value(s).
   */
  def zero(aggBuffer: MutableRow): Unit

  /**
   * Updates aggregation buffer with new `input` row.
   */
  def update(input: Row, aggBuffer: MutableRow): Unit

  /**
   * Merges values in aggregation buffers `fromAggBuffer` and `toAggBuffer`, then writes merged
   * values into `toAggBuffer`.
   */
  def merge(fromAggBuffer: Row, toAggBuffer: MutableRow): Unit

  /**
   * Evaluates the final result value using values in aggregation buffer `aggBuffer`, then writes it
   * into the `ordinal`-th field of `resultBuffer`.
   */
  def result(resultBuffer: MutableRow, ordinal: Int, aggBuffer: Row): Unit
}

case class DistinctAggregateFunction(child: AggregateFunction)
  extends AggregateFunction with UnaryExpression {

  override def dataType: DataType = child.dataType

  override def aggBufferSchema: StructType = error()

  override def supportPartialAggregation: Boolean = error()

  override def zero(aggBuffer: MutableRow): Unit = error()

  override def update(input: Row, aggBuffer: MutableRow): Unit = error()

  override def merge(fromAggBuffer: Row, intoAggBuffer: MutableRow): Unit = error()

  override def result(into: MutableRow, ordinal: Int, from: Row): Unit = error()

  override def sql: Try[String] = for {
    argSQL <- trySequence(child.children.map(_.sql))
    name = child.nodeName.toUpperCase
  } yield s"$name(DISTINCT ${argSQL mkString ", "})"

  override def debugString: String = {
    val args = child.children map (_.debugString)
    val name = child.nodeName.toUpperCase
    s"$name(DISTINCT ${args mkString ", "})"
  }

  private def error(): Nothing = throw new BrokenContractException(
    s"${getClass.getName} cannot be evaluated directly."
  )
}

trait DeclarativeAggregateFunction extends AggregateFunction {
  def aggBufferAttributes: Seq[AttributeRef]

  def zeroValues: Seq[Expression] = zeroValue :: Nil

  def zeroValue: Expression = throw new BrokenContractException(
    s"${getClass.getName} must override either zeroValue or zeroValues."
  )

  def updateExpressions: Seq[Expression] = updateExpression :: Nil

  def updateExpression: Expression = throw new BrokenContractException(
    s"${getClass.getName} must override either updateExpression or updateExpressions."
  )

  def mergeExpressions: Seq[Expression] = mergeExpression :: Nil

  def mergeExpression: Expression = throw new BrokenContractException(
    s"${getClass.getName} must override either mergeExpression or mergeExpressions."
  )

  def resultExpression: Expression

  override final lazy val aggBufferSchema: StructType =
    StructType.fromAttributes(aggBufferAttributes)

  override def zero(aggBuffer: MutableRow): Unit = {
    // Checks that all child expressions are bound right before evaluating this aggregate function.
    assertAllChildrenBound()
    aggBuffer.indices foreach { i =>
      aggBuffer(i) = zeroValues(i).evaluated
    }
  }

  override def update(input: Row, aggBuffer: MutableRow): Unit = updater(aggBuffer, input)

  override def merge(fromAggBuffer: Row, toAggBuffer: MutableRow): Unit =
    merger(toAggBuffer, fromAggBuffer)

  override def result(resultBuffer: MutableRow, ordinal: Int, aggBuffer: Row): Unit =
    resultBuffer(ordinal) = boundResultExpression evaluate aggBuffer

  private lazy val inputAggBufferAttributes = aggBufferAttributes map (_ withID newExpressionID())

  private lazy val joinedRow: JoinedRow = new JoinedRow()

  private lazy val boundUpdateExpressions: Seq[Expression] = updateExpressions map bind

  private lazy val boundMergeExpressions: Seq[Expression] = mergeExpressions map bind

  private lazy val boundResultExpression: Expression = bind(resultExpression)

  private lazy val updater = updateAggBufferWith(boundUpdateExpressions) _

  private lazy val merger = updateAggBufferWith(boundMergeExpressions) _

  /**
   * Updates mutable `aggBuffer` in-place using `expressions` and an `input` row.
   *
   *  1. Join `aggBuffer` and `input` into a single [[JoinedRow]];
   *  2. Use the [[JoinedRow]] as input row to evaluate all given `expressions` to produce `n`
   *     result values, where `n` is the length `aggBuffer` (and `expression`);
   *  3. Update the i-th cell of `aggBuffer` using the i-th evaluated result value.
   *
   * Pre-condition: Length of `expressions` must be equal to length of `aggBuffer`.
   */
  private def updateAggBufferWith(
    expressions: Seq[Expression]
  )(
    aggBuffer: MutableRow, input: Row
  ): Unit = {
    require(expressions.length == aggBuffer.length)
    val row = joinedRow(aggBuffer, input)
    aggBuffer.indices foreach (i => aggBuffer(i) = expressions(i) evaluate row)
  }

  // Used to bind update, merge, and result expressions. Note that we have the following constraints
  // for `DeclarativeAggregateFunction`:
  //
  //  1. All children expressions must be bound
  //  2. All expressions in `updateExpressions`, `mergeExpressions`, and `resultExpression` must be
  //     resolved but unbound.
  //
  // Thus, `AttributeRef`s must be aggregation buffer attributes, and `BoundRef`s only appear in
  // child expressions.
  private def bind(expression: Expression): Expression = expression transformDown {
    case ref: AttributeRef => BoundRef.bind(aggBufferAttributes ++ inputAggBufferAttributes)(ref)
    case ref: BoundRef     => ref at (ref.ordinal + aggBufferAttributes.length)
  }

  private def assertAllChildrenBound(): Unit = {
    children foreach { child =>
      child.collectFirst {
        case a: Attribute =>
          throw new BrokenContractException(
            s"""Attribute $a in child expression $child of aggregate function $this
               |hasn't been bound yet
             """.straight
          )
      }
    }
  }

  protected implicit class AggBufferAttribute(val left: AttributeRef) {
    def right: AttributeRef = inputAggBufferAttributes(aggBufferAttributes.indexOf(left))
  }
}

abstract class ReduceLeft(updateFunction: (Expression, Expression) => Expression)
  extends UnaryExpression with DeclarativeAggregateFunction {

  override def dataType: DataType = child.dataType

  override def isNullable: Boolean = child.isNullable

  protected lazy val value = 'value of dataType withNullability isNullable

  override def aggBufferAttributes: Seq[AttributeRef] = Seq(value)

  override def zeroValues: Seq[Expression] = Seq(lit(null) cast dataType)

  override def updateExpressions: Seq[Expression] = Seq(
    coalesce(updateFunction(value, child), value, child)
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    updateFunction(value.left, value.right)
  )

  override def resultExpression: Expression = value
}

case class Count(child: Expression) extends UnaryExpression with DeclarativeAggregateFunction {
  override def dataType: DataType = LongType

  override lazy val isNullable: Boolean = false

  private lazy val count = ('count of dataType).!

  override def aggBufferAttributes: Seq[AttributeRef] = Seq(count)

  override def zeroValue: Expression = 0L

  override def updateExpression: Expression = If(child.isNull, count, count + 1L)

  override def mergeExpression: Expression = count.left + count.right

  override def resultExpression: Expression = count
}

case class Average(child: Expression) extends UnaryExpression with DeclarativeAggregateFunction {
  override def nodeName: String = "AVG"

  override def dataType: DataType = DoubleType

  private lazy val sum = 'sum of dataType withNullability child.isNullable

  private lazy val count = 'count.long.!

  override def aggBufferAttributes: Seq[AttributeRef] = Seq(sum, count)

  override def zeroValues: Seq[Expression] = Seq(lit(null) cast child.dataType, 0L)

  override def updateExpressions: Seq[Expression] = Seq(
    coalesce((child cast dataType) + sum, child cast dataType, sum),
    count + If(child.isNull, 0L, 1L)
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    coalesce(sum.left + sum.right, sum.left, sum.right),
    coalesce(count.left + count.right, count.left, count.right)
  )

  override def resultExpression: Expression =
    If(count =:= 0L, lit(null), sum / (count cast dataType))

  override protected lazy val typeConstraint: TypeConstraint = Seq(child) sameSubtypeOf NumericType
}

case class First(child: Expression) extends ReduceLeft(coalesce(_, _))

case class Last(child: Expression) extends ReduceLeft(
  (agg: Expression, input: Expression) => coalesce(input, agg)
)

abstract class NumericReduceLeft(updateFunction: (Expression, Expression) => Expression)
  extends ReduceLeft(updateFunction) {

  override protected lazy val typeConstraint: TypeConstraint = Seq(child) sameSubtypeOf NumericType
}

case class Sum(child: Expression) extends NumericReduceLeft(Plus)

case class Max(child: Expression) extends NumericReduceLeft(Greatest(_, _))

case class Min(child: Expression) extends NumericReduceLeft(Least(_, _))

abstract class LogicalReduceLeft(updateFunction: (Expression, Expression) => Expression)
  extends ReduceLeft(updateFunction) {

  override protected lazy val typeConstraint: TypeConstraint = Seq(child) sameTypeAs BooleanType
}

case class BoolAnd(child: Expression) extends LogicalReduceLeft(And)

case class BoolOr(child: Expression) extends LogicalReduceLeft(Or)
