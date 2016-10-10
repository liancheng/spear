package scraper.expressions.aggregates

import scala.reflect.runtime.universe.WeakTypeTag

import scraper._
import scraper.exceptions.ContractBrokenException
import scraper.execution.MutableProjection
import scraper.expressions._
import scraper.expressions.NamedExpression.newExpressionID
import scraper.expressions.aggregates.FoldLeft.{MergeFunction, UpdateFunction}
import scraper.expressions.functions._
import scraper.types._

/**
 * A trait for aggregate functions, which aggregate grouped values into scalar values. While being
 * evaluated, an aggregation state, which is essentially a [[MutableRow]], is used to store
 * intermediate aggregated values. An aggregation state for an [[AggregateFunction]] may have
 * multiple fields. For example, [[Average]] uses two fields to store sum and total count of all
 * input values seen so far.
 */
trait AggregateFunction extends Expression with UnevaluableExpression {
  /**
   * Number of fields occupied by the aggregation state in the aggregation buffer.
   */
  def stateWidth: Int

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
   * Returns the final result value using values in `state`.
   */
  def result(state: Row): Any

  def distinct: DistinctAggregateFunction = DistinctAggregateFunction(this)
}

trait DuplicateInsensitive { this: AggregateFunction => }

/**
 * A helper class that represents distinct aggregate functions. This class should not appear in
 * analyzed logical plans since the analyzer is responsible for resolving it into normal aggregate
 * functions.
 */
case class DistinctAggregateFunction(child: AggregateFunction)
  extends AggregateFunction with UnaryExpression {

  override def dataType: DataType = child.dataType

  override def stateWidth: Int = child.stateWidth

  override def supportPartialAggregation: Boolean = child.supportPartialAggregation

  override def zero(state: MutableRow): Unit = bugReport()

  override def update(state: MutableRow, input: Row): Unit = bugReport()

  override def merge(state: MutableRow, inputState: Row): Unit = bugReport()

  override def result(state: Row): Any = bugReport()

  override protected def template(childList: Seq[String]): String =
    childList mkString (s"${child.nodeName.casePreserving}(DISTINCT ", ", ", ")")

  private def bugReport(): Nothing = throw new ContractBrokenException(
    "This method should never be invoked. You probably hit an internal bug."
  )
}

trait DeclarativeAggregateFunction extends AggregateFunction {
  val stateAttributes: Seq[AttributeRef]

  final lazy val inputStateAttributes: Seq[AttributeRef] =
    stateAttributes map (_ withID newExpressionID())

  /**
   * Initial literal values of aggregation state fields.
   */
  val zeroValues: Seq[Expression]

  /**
   * Resolved expressions used to update aggregation state fields, must not contain any
   * [[BoundRef]]s.
   */
  val updateExpressions: Seq[Expression]

  /**
   * Resolved expressions used to merge two aggregation state fields, must not contain any
   * [[BoundRef]]s.
   */
  val mergeExpressions: Seq[Expression]

  /**
   * Resolved expression used to compute the final aggregation result, must not contain any
   * [[BoundRef]]s.
   */
  val resultExpression: Expression

  override final lazy val stateWidth: Int = stateAttributes.length

  override def zero(state: MutableRow): Unit = zeroProjection target state apply ()

  override def update(state: MutableRow, input: Row): Unit =
    updateProjection target state apply joinedRow(state, input)

  override def merge(state: MutableRow, inputState: Row): Unit =
    mergeProjection target state apply joinedRow(state, inputState)

  override def result(state: Row): Any = bind(resultExpression) evaluate state

  protected implicit class StateAttribute(val left: AttributeRef) {
    def right: AttributeRef = inputStateAttributes(stateAttributes indexOf left)
  }

  private lazy val joinedRow: JoinedRow = new JoinedRow()

  private lazy val zeroProjection: MutableProjection = whenBound {
    MutableProjection(zeroValues)
  }

  private lazy val updateProjection: MutableProjection = whenBound {
    MutableProjection(updateExpressions map bind)
  }

  private lazy val mergeProjection: MutableProjection = whenBound {
    MutableProjection(mergeExpressions map bind)
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
  protected def bind(expression: Expression): Expression = expression transformDown {
    case ref: AttributeRef =>
      // Must be an aggregation state attribute of either the current aggregation state, which
      // appears in `stateAttributes`, or the input aggregation state to be merged, which appears in
      // `inputStateAttributes`.
      //
      // Note that here we also rely on the fact that `inputStateAttributes` are only used while
      // merging two aggregation states. They always appear on the right side of `stateAttributes`.
      BoundRef.bindTo(stateAttributes ++ inputStateAttributes)(ref)

    case ref: BoundRef =>
      // Must be a `BoundRef` appearing in the child expressions. Shifts the ordinal since input
      // rows are always appended to the right side of aggregation states.
      ref shift stateAttributes.length
  }
}

abstract class ImperativeAggregateFunction[T: WeakTypeTag] extends DeclarativeAggregateFunction {
  override final lazy val stateAttributes: Seq[AttributeRef] = Seq(state)

  override final lazy val zeroValues: Seq[Expression] = Seq(
    Literal(evaluator.initialState, stateType)
  )

  override final lazy val updateExpressions: Seq[Expression] = Seq(
    // TODO Eliminates the `struct` call
    // This scheme can be inefficient and hard to optimize in the future.
    evaluatorLit.invoke("update", stateType).withArgs(state, struct(children))
  )

  override final lazy val mergeExpressions: Seq[Expression] = Seq(
    evaluatorLit.invoke("merge", stateType).withArgs(state.left, state.right)
  )

  override final lazy val resultExpression: Expression = whenBound {
    evaluatorLit.invoke("result", dataType).withArgs(state)
  }

  protected type State = T

  protected trait Evaluator {
    def initialState: State

    def update(state: State, input: Row): State

    def merge(state: State, inputState: State): State

    def result(state: State): Any
  }

  protected val evaluator: Evaluator

  private def evaluatorLit: Literal = Literal(evaluator, ObjectType(classOf[Evaluator].getName))

  private val stateType: ObjectType = {
    val tag = implicitly[WeakTypeTag[T]]
    val runtimeClass = tag.mirror.runtimeClass(tag.tpe.typeSymbol.asClass)
    ObjectType(runtimeClass.getName)
  }

  private val state = 'state of stateType.!
}

trait FoldLeft extends UnaryExpression with DeclarativeAggregateFunction {
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

trait NullableReduceLeft extends FoldLeft {
  override lazy val zeroValue: Expression = Literal(null, child.dataType)

  override def mergeFunction: MergeFunction = updateFunction
}
