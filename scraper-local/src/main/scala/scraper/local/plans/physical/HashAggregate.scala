package scraper.local.plans.physical

import scala.collection.mutable

import scraper._
import scraper.execution.MutableProjection
import scraper.expressions._
import scraper.expressions.BoundRef.bindTo
import scraper.expressions.aggregates.DeclarativeAggregateFunction
import scraper.plans.physical.{PhysicalPlan, UnaryPhysicalPlan}

case class HashAggregate(
  child: PhysicalPlan,
  keyAliases: Seq[GroupingAlias],
  aggAliases: Seq[AggregationAlias]
) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = (keyAliases ++ aggAliases) map (_.attr)

  private lazy val boundKeys: Seq[Expression] = keyAliases map (_.child) map bindTo(child.output)

  private lazy val aggregator: Aggregator = {
    val boundAggs = aggAliases map (_.child) map bindTo(child.output)
    new Aggregator(boundAggs map (_.asInstanceOf[DeclarativeAggregateFunction]))
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

class Aggregator(aggs: Seq[DeclarativeAggregateFunction]) {
  def newAggregationBuffer(): MutableRow = {
    val buffer = new BasicMutableRow(aggs.map(_.stateAttributes.length).sum)
    zeroProjection.target(buffer).apply()
    buffer
  }

  def newResultBuffer(): MutableRow = new BasicMutableRow(aggs.length)

  def update(aggBuffer: MutableRow, input: Row): Unit =
    updateProjection target aggBuffer apply join(aggBuffer, input)

  def result(resultBuffer: MutableRow, aggBuffer: Row): Unit =
    resultProjection target resultBuffer apply aggBuffer

  private val stateAttributes: Seq[AttributeRef] = aggs.zipWithIndex.flatMap {
    case (agg, index) => agg.stateAttributes.map(renameStateAttributes(_, index))
  }

  private val inputStateAttributes: Seq[AttributeRef] = aggs.zipWithIndex.flatMap {
    case (agg, index) => agg.inputStateAttributes.map(renameStateAttributes(_, index))
  }

  private val join: JoinedRow = new JoinedRow()

  private val zeroProjection: MutableProjection = MutableProjection(aggs.flatMap(_.zeroValues))

  private val updateProjection: MutableProjection = MutableProjection(
    aggs.zipWithIndex.flatMap {
      case (agg, index) => agg.updateExpressions.map(renameStateAttributes(_, index)).map(bind)
    }
  )

  private val resultProjection: MutableProjection = MutableProjection(
    aggs.zipWithIndex.map {
      case (agg, index) => bind(renameStateAttributes(agg.resultExpression, index))
    }
  )

  private def renameStateAttributes[E <: Expression](e: E, index: Int): E = e.transformUp {
    case ref: AttributeRef => ref.copy(name = ref.name append s"_agg$index")
  }.asInstanceOf[E]

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
      BoundRef.bindTo(stateAttributes ++ inputStateAttributes)(ref)

    case ref: BoundRef =>
      // Must be a `BoundRef` appearing in the child expressions. Shifts the ordinal since input
      // rows are always appended to the right side of aggregation states.
      ref shift stateAttributes.length
  }
}
