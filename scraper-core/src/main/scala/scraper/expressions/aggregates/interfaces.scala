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
 * evaluated, an aggregation state, which is essentially a [[MutableRow]], is used to store
 * intermediate aggregated values. An aggregation state for an [[AggregateFunction]] may have
 * multiple fields. For example, [[Average]] uses two fields to store sum and total count of all
 * input values seen so far.
 */
trait AggregateFunction extends Expression with UnevaluableExpression {
  /**
   * Schema of the aggregation state.
   */
  def stateSchema: StructType

  /**
   * Whether this [[AggregateFunction]] supports partial aggregation.
   */
  def supportPartialAggregation: Boolean = true

  /**
   * Initializes the aggregation `state` with zero value(s).
   */
  def zero(state: MutableRow): Unit

  /**
   * Updates aggregation `state` with new `input` row.
   */
  def update(state: MutableRow, input: Row): Unit

  /**
   * Merges another aggregation state into an existing aggregation state.
   */
  def merge(state: MutableRow, inputState: Row): Unit

  /**
   * Evaluates the final result value using values in `state`, then writes it into the `ordinal`-th
   * field of `resultBuffer`.
   */
  def result(resultBuffer: MutableRow, ordinal: Int, state: Row): Unit
}

case class DistinctAggregateFunction(child: AggregateFunction)
  extends AggregateFunction with UnaryExpression {

  override def dataType: DataType = child.dataType

  override def stateSchema: StructType = child.stateSchema

  override def supportPartialAggregation: Boolean = child.supportPartialAggregation

  override def zero(state: MutableRow): Unit = bugReport()

  override def update(state: MutableRow, input: Row): Unit = bugReport()

  override def merge(state: MutableRow, inputState: Row): Unit = bugReport()

  override def result(resultBuffer: MutableRow, ordinal: Int, state: Row): Unit = bugReport()

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
  val stateAttributes: Seq[AttributeRef]

  /**
   * Initial values aggregation state fields. Must be literals.
   */
  val zeroValues: Seq[Expression]

  /**
   * Expressions used to update aggregation state fields. All expressions must be either literals
   * or resolved but unbound expressions.
   */
  val updateExpressions: Seq[Expression]

  /**
   * Expressions used to merge two aggregation state fields. All expressions must be either
   * literals or resolved but unbound expressions.
   */
  val mergeExpressions: Seq[Expression]

  /**
   * Expression used to compute the final aggregation result. The expression must be either literals
   * or resolved but unbound expressions.
   */
  val resultExpression: Expression

  override final lazy val stateSchema: StructType = StructType.fromAttributes(stateAttributes)

  override def zero(state: MutableRow): Unit =
    state.indices foreach (i => state(i) = zeroValues(i).evaluated)

  override def update(state: MutableRow, input: Row): Unit = updater(state, input)

  override def merge(state: MutableRow, inputState: Row): Unit = merger(state, inputState)

  override def result(resultBuffer: MutableRow, ordinal: Int, state: Row): Unit =
    resultBuffer(ordinal) = boundResultExpression evaluate state

  protected implicit class StateAttribute(val left: AttributeRef) {
    def right: AttributeRef = inputStateAttributes(stateAttributes indexOf left)
  }

  private lazy val inputStateAttributes = stateAttributes map (_ withID newExpressionID())

  private lazy val joinedRow: JoinedRow = new JoinedRow()

  private lazy val boundResultExpression: Expression = whenBound {
    bind(resultExpression)
  }

  private lazy val updater: (MutableRow, Row) => Unit = whenBound {
    val boundUpdateExpressions = updateExpressions map bind
    updateStateWith(boundUpdateExpressions)
  }

  private lazy val merger: (MutableRow, Row) => Unit = whenBound {
    val boundMergeExpressions = mergeExpressions map bind
    updateStateWith(boundMergeExpressions)
  }

  // Updates the mutable `state` in-place with values evaluated using given `expressions` and an
  // `input` row.
  //
  // Pre-condition: Length of `expressions` must be equal to length of `state`.
  private def updateStateWith(expressions: Seq[Expression])(state: MutableRow, input: Row): Unit = {
    require(expressions.length == state.length)
    val row = joinedRow(state, input)
    state.indices foreach (i => state(i) = expressions(i) evaluate row)
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
  // Thus, all `AttributeRef`s found in the target expression must be aggregation state attributes,
  // while all `BoundRef`s found in the target expression only appear in child expressions.
  private def bind(expression: Expression): Expression = expression transformDown {
    case ref: AttributeRef =>
      // Must be an aggregation state attribute of either the current aggregation state, which
      // appears in `stateAttributes`, or the input aggregation state to be merged, which appears in
      // `inputStateAttributes`.
      //
      // Note that here we also rely on the fact that `inputStateAttributes` are only used while
      // merging two aggregation states. They always appear on the right side of `stateAttributes`.
      BoundRef.bind(stateAttributes ++ inputStateAttributes)(ref)

    case ref: BoundRef =>
      // Must be a `BoundRef` appearing in the child expressions. Shifts the ordinal since input
      // rows are always appended to the right side of aggregation states.
      ref at (ref.ordinal + stateAttributes.length)
  }
}

abstract class FoldLeft extends UnaryExpression with DeclarativeAggregateFunction {
  def zeroValue: Expression

  def updateFunction: UpdateFunction

  def mergeFunction: MergeFunction

  override def isNullable: Boolean = child.isNullable

  override lazy val dataType: DataType = zeroValue.dataType

  override lazy val zeroValues: Seq[Expression] = Seq(zeroValue)

  override lazy val stateAttributes: Seq[AttributeRef] = Seq(value)

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
