package scraper.expressions.aggregates

import scala.util.Try

import scraper.{JoinedRow, MutableRow, Row}
import scraper.exceptions.ContractBrokenException
import scraper.expressions._
import scraper.expressions.NamedExpression.newExpressionID
import scraper.expressions.aggregates.FoldLeft.{MergeFunction, UpdateFunction}
import scraper.expressions.functions._
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
   * Expressions used to update aggregation buffer fields. All expressions must be either literals
   * or resolved but unbound expressions.
   */
  val updateExpressions: Seq[Expression]

  /**
   * Expressions used to merge two aggregation buffers fields. All expressions must be either
   * literals or resolved but unbound expressions.
   */
  val mergeExpressions: Seq[Expression]

  /**
   * Expression used to compute the final aggregation result. The expression must be either literals
   * or resolved but unbound expressions.
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
    bind(resultExpression)
  }

  private lazy val updater: (MutableRow, Row) => Unit = whenBound {
    val boundUpdateExpressions = updateExpressions map bind
    updateAggBufferWith(boundUpdateExpressions, _, _)
  }

  private lazy val merger: (MutableRow, Row) => Unit = whenBound {
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
  // One crucial pre-condition that always holds when binding these expressions is that:
  //
  //   All child expressions of this `DeclarativeAggregateFunction` must have been bound.
  //
  // This is because this method is only invoked while a `DeclarativeAggregateFunction` is being
  // evaluated, which implies the `DeclarativeAggregateFunction`, together with all its child
  // expressions, must have been bound.
  //
  // Thus, all `AttributeRef`s found in the target expression must be aggregation buffer attributes,
  // while all `BoundRef`s found in the target expression only appear in child expressions.
  private def bind(expression: Expression): Expression = expression transformDown {
    case ref: AttributeRef =>
      // Must be an aggregation buffer attribute of either the current aggregation buffer, which
      // appears in `aggBufferAttributes`, or the input aggregation buffer to be merged, which
      // appears in `inputAggBufferAttributes`.
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

abstract class NullableReduceLeft extends FoldLeft {
  override lazy val zeroValue: Expression = Literal(null, child.dataType)

  override def mergeFunction: MergeFunction = updateFunction
}
