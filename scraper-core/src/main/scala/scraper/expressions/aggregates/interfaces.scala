package scraper.expressions.aggregates

import scala.reflect.runtime.universe.WeakTypeTag

import scraper._
import scraper.exceptions.ContractBrokenException
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
  val stateAttributes: Seq[Attribute]

  final lazy val inputStateAttributes: Seq[Attribute] =
    stateAttributes map (_ withID newExpressionID())

  /**
   * Initial literal values of aggregation state fields.
   */
  val initialValues: Seq[Expression]

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

  def distinct: DistinctAggregateFunction = DistinctAggregateFunction(this)

  protected implicit class StateAttribute(val left: Attribute) {
    def right: Attribute = inputStateAttributes(stateAttributes indexOf left)
  }
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

  override lazy val stateAttributes: Seq[Attribute] = bugReport()

  override lazy val initialValues: Seq[Expression] = bugReport()

  override lazy val updateExpressions: Seq[Expression] = bugReport()

  override lazy val mergeExpressions: Seq[Expression] = bugReport()

  override lazy val resultExpression: Expression = bugReport()

  override protected def template(childList: Seq[String]): String =
    childList mkString (s"${child.nodeName.casePreserving}(DISTINCT ", ", ", ")")

  private def bugReport(): Nothing = throw new ContractBrokenException(
    "This method should never be invoked. You probably hit an internal bug."
  )
}

abstract class ImperativeAggregateFunction[T: WeakTypeTag] extends AggregateFunction {
  override final lazy val stateAttributes: Seq[Attribute] = Seq(state)

  override final lazy val initialValues: Seq[Expression] = Seq(
    Literal(initialState, stateType)
  )

  override final lazy val updateExpressions: Seq[Expression] = Seq(
    // TODO Eliminates the `struct` call
    // This scheme can be inefficient and hard to optimize in the future.
    self.invoke("update", stateType).withArgs(state, struct(children))
  )

  override final lazy val mergeExpressions: Seq[Expression] = Seq(
    self.invoke("merge", stateType).withArgs(state.left, state.right)
  )

  override final lazy val resultExpression: Expression = whenBound {
    self.invoke("result", dataType).withArgs(state)
  }

  protected type State = T

  def initialState: State

  def update(state: State, input: Row): State

  def merge(state: State, inputState: State): State

  def result(state: State): Any

  private def self: Literal = Literal(this, ObjectType(getClass.getName))

  private val stateType: ObjectType = {
    val tag = implicitly[WeakTypeTag[T]]
    val runtimeClass = tag.mirror.runtimeClass(tag.tpe.typeSymbol.asClass)
    ObjectType(runtimeClass.getName)
  }

  private val state = 'state of stateType.!
}

trait FoldLeft extends UnaryExpression with AggregateFunction {
  def zeroValue: Expression

  def updateFunction: UpdateFunction

  def mergeFunction: MergeFunction

  override def isNullable: Boolean = child.isNullable

  override lazy val dataType: DataType = zeroValue.dataType

  override lazy val stateAttributes: Seq[Attribute] = Seq(value)

  override lazy val initialValues: Seq[Expression] = Seq(zeroValue)

  override lazy val updateExpressions: Seq[Expression] = Seq(
    coalesce(updateFunction(value, child), value, child)
  )

  override lazy val mergeExpressions: Seq[Expression] = Seq(
    mergeFunction(value.left, value.right)
  )

  override lazy val resultExpression: Expression = value

  protected lazy val value = 'value of dataType nullable isNullable
}

object FoldLeft {
  type UpdateFunction = (Expression, Expression) => Expression

  type MergeFunction = (Expression, Expression) => Expression
}

trait NullableReduceLeft extends FoldLeft {
  override lazy val zeroValue: Expression = Literal(null, child.dataType)

  override def mergeFunction: MergeFunction = updateFunction
}
