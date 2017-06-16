package spear.local.plans.physical

import scala.collection.mutable

import spear._
import spear.execution.MutableProjection
import spear.expressions._
import spear.expressions.aggregates.AggregateFunction
import spear.plans.physical.{PhysicalPlan, UnaryPhysicalPlan}

case class HashAggregate(
  child: PhysicalPlan,
  keyAliases: Seq[GroupingKeyAlias],
  aggAliases: Seq[AggregateFunctionAlias]
) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = (keyAliases ++ aggAliases) map { _.attr }

  override def requireMaterialization: Boolean = true

  private lazy val boundKeys: Seq[Expression] = keyAliases map { _.child bindTo child.output }

  private lazy val aggregator: Aggregator = new Aggregator(
    aggAliases
      .map { _.child bindTo child.output }
      .map { case e: AggregateFunction => e }
  )

  private lazy val hashMap = mutable.HashMap.empty[Row, MutableRow]

  override def iterator: Iterator[Row] = {
    // Builds the hash map by consuming all input rows
    child.iterator foreach { input =>
      val groupingRow = Row.fromSeq(boundKeys map { _ evaluate input })
      val stateBuffer = hashMap.getOrElseUpdate(groupingRow, aggregator.newStateBuffer())
      aggregator.update(stateBuffer, input)
    }

    val resultBuffer = aggregator.newResultBuffer()
    val join = new JoinedRow()

    hashMap.iterator map {
      case (groupingRow, stateBuffer) =>
        aggregator.result(resultBuffer, stateBuffer)
        join(groupingRow, resultBuffer)
    }
  }

  override protected def withExpressions(newExpressions: Seq[Expression]): PhysicalPlan = {
    val (newKeyAliases, newAggAliases) = newExpressions splitAt keyAliases.length

    copy(
      keyAliases = newKeyAliases.toArray map { case e: GroupingKeyAlias => e },
      aggAliases = newAggAliases.toArray map { case e: AggregateFunctionAlias => e }
    )
  }
}

class Aggregator(aggs: Seq[AggregateFunction]) {
  require(aggs.forall { _.isBound })

  def newStateBuffer(): MutableRow = {
    val buffer = new BasicMutableRow(aggs.map { _.stateAttributes.length }.sum)
    initializationProjection target buffer apply ()
    buffer
  }

  def newResultBuffer(): MutableRow = new BasicMutableRow(aggs.length)

  def update(stateBuffer: MutableRow, input: Row): Unit =
    updateProjection target stateBuffer apply join(stateBuffer, input)

  def result(resultBuffer: MutableRow, aggBuffer: Row): Unit =
    resultProjection target resultBuffer apply aggBuffer

  // Binds `updateExpressions`, `mergeExpressions`, and `resultExpression` of all involved aggregate
  // functions.
  private val bind: Expression => Expression = {
    // Concatenates all state attributes and all input state attributes of all aggregate functions
    // respectively.
    //
    // While merging an input state buffer into a target state buffer, we do a mutable projection
    // over a `JoinedRow`, which joins the target state buffer on the left side and the input state
    // buffer on the right side:
    //
    //           MutableRow                  Row
    //   |<- - - - - - - - - - - >|<- - - - - - - - - - - >|
    //   +------------------------+------------------------+
    //   |  target state buffer   |   input state buffer   |
    //   |   (stateAttributes)    | (inputStateAttributes) |
    //   +------------------------+------------------------+
    //   |<- - - - - - - - - - - - - - - - - - - - - - - ->|
    //                        JoinedRow
    //
    // Here, `stateAttributes` and `inputStateAttributes` represent columns of the target and input
    // state buffer respectively.
    val stateAttributes = aggs flatMap { _.stateAttributes }
    val inputStateAttributes = aggs flatMap { _.inputStateAttributes }

    (_: Expression) transformDown {
      case ref: AttributeRef =>
        // Binds all aggregation state attributes to corresponding fields of the target and input
        // state buffers.
        ref bindTo stateAttributes ++ inputStateAttributes

      case ref: BoundRef =>
        // While updating a target state buffer using an input row, we do a mutable projection over
        // a `JoinedRow`, which joins the target state buffer on the left side and the input row on
        // the right side:
        //
        //           MutableRow              Row
        //   |<- - - - - - - - - - - >|<- - - - - - - >|
        //   +------------------------+----------------+
        //   |  target state buffer   |   input row    |
        //   |   (stateAttributes)    | (child.output) |
        //   +------------------------+----------------+
        //   |<- - - - - - - - - - - - - - - - - - - ->|
        //                    JoinedRow
        //
        // Note that when `bind` is being invoked, child expressions of all aggregate functions
        // involved here must have been bound, while their `updateExpressions`, `mergeExpressions`,
        // and `resultExpression` must have NOT been bound. Therefore, all `BoundRef`s found here
        // must belong to child expressions of involved aggregate functions, and we only need to
        // shift their ordinals to the right side so that they are bound to the corresponding input
        // row fields.
        ref shift stateAttributes.length
    }
  }

  private val join: JoinedRow = new JoinedRow()

  private val initializationProjection: MutableProjection = MutableProjection(
    aggs flatMap { _.initialValues }
  )

  private val updateProjection: MutableProjection = MutableProjection(
    aggs flatMap { _.updateExpressions } map bind
  )

  private val resultProjection: MutableProjection = MutableProjection(
    aggs map { _.resultExpression } map bind
  )
}
