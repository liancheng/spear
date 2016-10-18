package scraper.local.plans.physical

import scala.collection.mutable

import scraper._
import scraper.execution.MutableProjection
import scraper.expressions._
import scraper.expressions.BoundRef.bindTo
import scraper.expressions.aggregates.AggregateFunction
import scraper.plans.physical.{PhysicalPlan, UnaryPhysicalPlan}

case class HashAggregate(
  child: PhysicalPlan,
  keyAliases: Seq[GroupingAlias],
  aggAliases: Seq[AggregationAlias]
) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = (keyAliases ++ aggAliases) map (_.attr)

  override def needCopy: Boolean = true

  private lazy val boundKeys: Seq[Expression] = keyAliases map (_.child) map bindTo(child.output)

  private lazy val aggregator: Aggregator = {
    val boundAggs = aggAliases map (_.child) map bindTo(child.output)
    new Aggregator(boundAggs)
  }

  private lazy val hashMap = mutable.HashMap.empty[Row, MutableRow]

  override def iterator: Iterator[Row] = {
    // Builds the hash map by consuming all input rows
    child.iterator foreach { input =>
      val groupingRow = Row.fromSeq(boundKeys map (_ evaluate input))
      val aggBuffer = hashMap.getOrElseUpdate(groupingRow, aggregator.newAggregationBuffer())
      aggregator.update(aggBuffer, input)
    }

    val resultBuffer = aggregator.newResultBuffer()
    val join = new JoinedRow()

    hashMap.iterator map {
      case (groupingRow, aggBuffer) =>
        aggregator.result(resultBuffer, aggBuffer)
        join(groupingRow, resultBuffer)
    }
  }
}

class Aggregator(aggs: Seq[AggregateFunction]) {
  require(aggs.forall(_.isBound))

  def newAggregationBuffer(): MutableRow = {
    val buffer = new BasicMutableRow(aggs.map(_.stateAttributes.length).sum)
    initializationProjection target buffer apply ()
    buffer
  }

  def newResultBuffer(): MutableRow = new BasicMutableRow(aggs.length)

  def update(aggBuffer: MutableRow, input: Row): Unit =
    updateProjection target aggBuffer apply join(aggBuffer, input)

  def result(resultBuffer: MutableRow, aggBuffer: Row): Unit =
    resultProjection target resultBuffer apply aggBuffer

  private val bind: Expression => Expression = {
    val stateAttributes = aggs flatMap (_.stateAttributes)

    val inputStateAttributes = aggs flatMap (_.inputStateAttributes)

    (_: Expression).transformDown {
      case ref: AggStateAttribute =>
        BoundRef.bindTo(stateAttributes ++ inputStateAttributes)(ref)

      case ref: BoundRef =>
        ref shift stateAttributes.length
    }
  }

  private val join: JoinedRow = new JoinedRow()

  private val initializationProjection: MutableProjection = MutableProjection(
    aggs flatMap (_.initialValues)
  )

  private val updateProjection: MutableProjection = MutableProjection(
    aggs flatMap (_.updateExpressions) map bind
  )

  private val resultProjection: MutableProjection = MutableProjection(
    aggs map (_.resultExpression) map bind
  )
}
