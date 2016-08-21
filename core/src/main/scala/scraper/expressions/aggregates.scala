package scraper.expressions

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import scraper.{JoinedRow, MutableRow, Name, Row}
import scraper.exceptions.ContractBrokenException
import scraper.expressions.FoldLeft.{MergeFunction, UpdateFunction}
import scraper.expressions.NamedExpression.newExpressionID
import scraper.expressions.functions._
import scraper.expressions.typecheck.TypeConstraint
import scraper.types._
import scraper.utils._

/**
 * A trait for aggregate functions, which aggregate grouped values into scalar values. While being
 * evaluated, an aggregation buffer, which is essentially a [[MutableRow]], is used to store
 * aggregated intermediate values. An aggregation buffer for an [[AggregateFunction]] may have
 * multiple fields. For example, [[Average]] uses two fields to store sum and total count of all
 * input values seen so far.
 */
trait AggregateFunction extends Expression with UnevaluableExpression {
  /**
   * Schema of the aggregation buffer.
   */
  def aggBufferSchema: StructType

  /**
   * Whether this [[AggregateFunction]] supports partial aggregation
   */
  def supportsPartialAggregation: Boolean = true

  /**
   * Initializes the aggregation buffer with zero value(s).
   */
  def zero(aggBuffer: MutableRow): Unit

  /**
   * Updates aggregation buffer with new `input` row.
   */
  def update(aggBuffer: MutableRow, input: Row): Unit

  /**
   * Merges another aggregation buffer into an existing aggregation buffer.
   */
  def merge(aggBuffer: MutableRow, inputAggBuffer: Row): Unit

  /**
   * Evaluates the final result value using values in aggregation buffer `aggBuffer`, then writes it
   * into the `ordinal`-th field of `resultBuffer`.
   */
  def result(resultBuffer: MutableRow, ordinal: Int, aggBuffer: Row): Unit
}

abstract class Collect(child: Expression) extends AggregateFunction with UnaryExpression {
  override def isNullable: Boolean = false

  override def aggBufferSchema: StructType = StructType('collection -> dataType.!)

  override protected lazy val strictDataType: DataType = ArrayType(child.dataType, child.isNullable)
}

case class CollectList(child: Expression) extends Collect(child) {
  override def nodeName: Name = "collect_list"

  override def zero(aggBuffer: MutableRow): Unit = aggBuffer(0) = ArrayBuffer.empty[Any]

  override def update(aggBuffer: MutableRow, input: Row): Unit = {
    aggBuffer.head.asInstanceOf[ArrayBuffer[Any]] += child.evaluate(input)
  }

  override def merge(aggBuffer: MutableRow, inputAggBuffer: Row): Unit = {
    val from = inputAggBuffer.head.asInstanceOf[ArrayBuffer[Any]]
    val into = aggBuffer.head.asInstanceOf[ArrayBuffer[Any]]
    into ++= from
  }

  override def result(resultBuffer: MutableRow, ordinal: Int, aggBuffer: Row): Unit =
    resultBuffer(ordinal) = aggBuffer.head.asInstanceOf[ArrayBuffer[Any]]
}

case class CollectSet(child: Expression) extends Collect(child) {
  override def nodeName: Name = "collect_set"

  override def zero(aggBuffer: MutableRow): Unit = aggBuffer(0) = mutable.Set.empty[Any]

  override def update(aggBuffer: MutableRow, input: Row): Unit = {
    aggBuffer.head.asInstanceOf[mutable.Set[Any]] += child.evaluate(input)
  }

  override def merge(aggBuffer: MutableRow, inputAggBuffer: Row): Unit = {
    val from = inputAggBuffer.head.asInstanceOf[mutable.Set[Any]]
    val into = aggBuffer.head.asInstanceOf[mutable.Set[Any]]
    into ++= from
  }

  override def result(resultBuffer: MutableRow, ordinal: Int, aggBuffer: Row): Unit =
    resultBuffer(ordinal) = aggBuffer.head.asInstanceOf[mutable.Set[Any]].toSeq
}

case class DistinctAggregateFunction(child: AggregateFunction)
  extends AggregateFunction with UnaryExpression {

  override def dataType: DataType = child.dataType

  override def aggBufferSchema: StructType = child.aggBufferSchema

  override def supportsPartialAggregation: Boolean = child.supportsPartialAggregation

  override def zero(aggBuffer: MutableRow): Unit = bugReport()

  override def update(aggBuffer: MutableRow, input: Row): Unit = bugReport()

  override def merge(aggBuffer: MutableRow, inputAggBuffer: Row): Unit = bugReport()

  override def result(resultBuffer: MutableRow, ordinal: Int, aggBuffer: Row): Unit = bugReport()

  override def sql: Try[String] = for {
    argSQL <- trySequence(child.children.map(_.sql))
    name = child.nodeName
  } yield s"$name(DISTINCT ${argSQL mkString ", "})"

  override def debugString: String = {
    val args = child.children map (_.debugString)
    val name = child.nodeName
    s"$name(DISTINCT ${args mkString ", "})"
  }

  private def bugReport(): Nothing = throw new ContractBrokenException(
    "This method should never be invoked. You probably hit an internal bug."
  )
}

trait DeclarativeAggregateFunction extends AggregateFunction {
  val aggBufferAttributes: Seq[AttributeRef]

  /**
   * Initial values aggregation buffer fields. Must be literals.
   */
  val zeroValues: Seq[Expression]

  /**
   * Expressions used to update aggregation buffer fields. All expressions must be resolved but
   * unbound.
   */
  val updateExpressions: Seq[Expression]

  /**
   * Expressions used to merge two aggregation buffers fields. All expressions must be resolved but
   * unbound.
   */
  val mergeExpressions: Seq[Expression]

  /**
   * Expression used to compute the final aggregation result. The expression must be resolved but
   * unbound.
   */
  val resultExpression: Expression

  override final lazy val aggBufferSchema: StructType =
    StructType.fromAttributes(aggBufferAttributes)

  override def zero(aggBuffer: MutableRow): Unit = {
    // Checks that all child expressions are bound right before evaluating this aggregate function.
    assertAllChildrenBound()
    aggBuffer.indices foreach (i => aggBuffer(i) = zeroValues(i).evaluated)
  }

  override def update(aggBuffer: MutableRow, input: Row): Unit = updater(aggBuffer, input)

  override def merge(aggBuffer: MutableRow, inputAggBuffer: Row): Unit =
    merger(aggBuffer, inputAggBuffer)

  override def result(resultBuffer: MutableRow, ordinal: Int, aggBuffer: Row): Unit =
    resultBuffer(ordinal) = boundResultExpression evaluate aggBuffer

  protected implicit class AggBufferAttribute(val left: AttributeRef) {
    def right: AttributeRef = inputAggBufferAttributes(aggBufferAttributes indexOf left)
  }

  private lazy val inputAggBufferAttributes = aggBufferAttributes map (_ withID newExpressionID())

  private lazy val joinedRow: JoinedRow = new JoinedRow()

  private lazy val boundResultExpression: Expression = whenBound {
    require(resultExpression.isResolved)
    require(!resultExpression.isBound)
    bind(resultExpression)
  }

  private lazy val updater: (MutableRow, Row) => Unit = whenBound {
    require(updateExpressions forall (_.isResolved))
    require(updateExpressions forall (!_.isBound))
    val boundUpdateExpressions = updateExpressions map bind
    updateAggBufferWith(boundUpdateExpressions, _, _)
  }

  private lazy val merger: (MutableRow, Row) => Unit = whenBound {
    require(mergeExpressions forall (_.isResolved))
    require(mergeExpressions forall (!_.isBound))
    val boundMergeExpressions = mergeExpressions map bind
    updateAggBufferWith(boundMergeExpressions, _, _)
  }

  // Updates the mutable `aggBuffer` in-place with values evaluated using given `expressions` and an
  // `input` row.
  //
  //  1. Joins `aggBuffer` and `input` into a single `JoinedRow`, with `aggBuffer` on the left hand
  //     side and `input` on the right hand side;
  //  2. Uses the `JoinedRow` as input row to evaluate all given `expressions` to produce `n`
  //     result values, where `n` is the length `aggBuffer` (and `expression`);
  //  3. Updates the i-th cell of `aggBuffer` using the i-th evaluated result value.
  //
  // Pre-condition: Length of `expressions` must be equal to length of `aggBuffer`.
  private def updateAggBufferWith(
    expressions: Seq[Expression],
    aggBuffer: MutableRow,
    input: Row
  ): Unit = {
    require(expressions.length == aggBuffer.length)
    val row = joinedRow(aggBuffer, input)
    aggBuffer.indices foreach (i => aggBuffer(i) = expressions(i) evaluate row)
  }

  // Used to bind the following expressions of a `DeclarativeAggregateFunction`:
  //
  //  - `updateExpressions`,
  //  - `mergeExpressions`, and
  //  - `resultExpression`
  //
  // Note that the following pre-conditions must hold before binding these expressions:
  //
  //  1. All children expressions of this `DeclarativeAggregateFunction` must have been bound.
  //
  //     Because this method is only invoked while a `DeclarativeAggregateFunction` is being
  //     evaluated, which implies the `DeclarativeAggregateFunction`, together with all its child
  //     expressions, must have been bound.
  //
  //  2. All expressions in `updateExpressions`, `mergeExpressions`, and `resultExpression` must be
  //     resolved but unbound.
  //
  //     This is an explicit contract defined by `DeclarativeAggregateFunction`.
  //
  // With these pre-conditions in mind, all `AttributeRef`s found in the target expression must be
  // aggregation buffer attributes, while all `BoundRef`s found in the target expression must appear
  // in child expressions.
  private def bind(expression: Expression): Expression = expression transformDown {
    case ref: AttributeRef =>
      // Must be an aggregation buffer attribute of either the buffer of the current aggregate
      // function, which appears in `aggBufferAttributes`, or the input aggregation buffer to be
      // merged, which appears in `inputAggBufferAttributes`.
      //
      // Note that here we also rely on the fact that `inputAggBufferAttributes` are only used while
      // merging two aggregation buffers. They always appear on the right side of
      // `aggBufferAttributes`.
      BoundRef.bind(aggBufferAttributes ++ inputAggBufferAttributes)(ref)

    case ref: BoundRef =>
      // Must be a `BoundRef` appearing in the child expressions. Shifts the ordinal since input
      // rows are always appended to the right side of aggregation buffers.
      ref at (ref.ordinal + aggBufferAttributes.length)
  }

  private def assertAllChildrenBound(): Unit = children foreach { child =>
    child.collectFirst {
      case a: Attribute =>
        throw new ContractBrokenException(
          s"""Attribute $a in child expression $child of aggregate function $this
             |hasn't been bound yet
           """.oneLine
        )
    }
  }
}

case class Average(child: Expression) extends UnaryExpression with DeclarativeAggregateFunction {
  override def nodeName: Name = "AVG"

  override def dataType: DataType = DoubleType

  override lazy val aggBufferAttributes: Seq[AttributeRef] = Seq(sum, count)

  override lazy val zeroValues: Seq[Expression] = Seq(Literal(null, child.dataType), 0L)

  override lazy val updateExpressions: Seq[Expression] = Seq(
    coalesce((child cast dataType) + sum, child cast dataType, sum),
    if (child.isNullable) If(child.isNull, count, count + 1L) else count + 1L
  )

  override lazy val mergeExpressions: Seq[Expression] = Seq(
    sum.left + sum.right,
    count.left + count.right
  )

  override lazy val resultExpression: Expression =
    If(count === 0L, lit(null), sum / (count cast dataType))

  override protected lazy val typeConstraint: TypeConstraint = Seq(child) sameSubtypeOf NumericType

  private lazy val sum = 'sum of dataType withNullability child.isNullable

  private lazy val count = 'count.long.!
}

abstract class FoldLeft extends UnaryExpression with DeclarativeAggregateFunction {
  def zeroValue: Expression

  def updateFunction: UpdateFunction

  def mergeFunction: MergeFunction

  override def isNullable: Boolean = child.isNullable

  override lazy val dataType: DataType = zeroValue.dataType

  override lazy val zeroValues: Seq[Expression] = Seq(zeroValue)

  override lazy val aggBufferAttributes: Seq[AttributeRef] = Seq(value)

  override lazy val updateExpressions: Seq[Expression] = Seq(
    coalesce(updateFunction(value, child), value, child)
  )

  override lazy val mergeExpressions: Seq[Expression] = Seq(
    mergeFunction(value.left, value.right)
  )

  override lazy val resultExpression: Expression = value

  protected lazy val value = 'value of dataType withNullability isNullable
}

object FoldLeft {
  type UpdateFunction = (Expression, Expression) => Expression

  type MergeFunction = (Expression, Expression) => Expression
}

case class Count(child: Expression) extends FoldLeft {
  override lazy val zeroValue: Expression = 0L

  override lazy val updateFunction: UpdateFunction = if (child.isNullable) {
    (count: Expression, input: Expression) => count + If(input.isNull, 0L, 1L)
  } else {
    (count: Expression, _) => count + 1L
  }

  override def mergeFunction: MergeFunction = _ + _

  override protected lazy val value = 'value of dataType.!
}

abstract class NullableReduceLeft extends FoldLeft {
  override lazy val zeroValue: Expression = Literal(null, child.dataType)

  override def mergeFunction: MergeFunction = updateFunction
}

case class Max(child: Expression) extends NullableReduceLeft {
  override val updateFunction: UpdateFunction = Greatest(_, _)

  override protected lazy val typeConstraint: TypeConstraint = children sameSubtypeOf OrderedType
}

case class Min(child: Expression) extends NullableReduceLeft {
  override val updateFunction: UpdateFunction = Least(_, _)

  override protected lazy val typeConstraint: TypeConstraint = children sameSubtypeOf OrderedType
}

case class FirstValue(child: Expression) extends NullableReduceLeft {
  override def nodeName: Name = "first_value"

  override val updateFunction: UpdateFunction = coalesce(_, _)
}

case class LastValue(child: Expression) extends NullableReduceLeft {
  override def nodeName: Name = "last_value"

  override val updateFunction: UpdateFunction =
    (last: Expression, input: Expression) => coalesce(input, last)
}

abstract class NumericNullableReduceLeft extends NullableReduceLeft {
  override protected lazy val typeConstraint: TypeConstraint = children sameSubtypeOf NumericType
}

case class Sum(child: Expression) extends NumericNullableReduceLeft {
  override val updateFunction: UpdateFunction = Plus
}

case class Product_(child: Expression) extends NumericNullableReduceLeft {
  override def nodeName: Name = "product"

  override val updateFunction: UpdateFunction = Multiply
}

abstract class LogicalNullableReduceLeft extends NullableReduceLeft {
  override protected lazy val typeConstraint: TypeConstraint = children sameTypeAs BooleanType
}

case class BoolAnd(child: Expression) extends LogicalNullableReduceLeft {
  override def nodeName: Name = "bool_and"

  override val updateFunction: UpdateFunction = And
}

case class BoolOr(child: Expression) extends LogicalNullableReduceLeft {
  override def nodeName: Name = "bool_or"

  override val updateFunction: UpdateFunction = Or
}
