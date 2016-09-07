package scraper.local.plans.physical

import scala.collection.mutable

import scraper._
import scraper.expressions._
import scraper.expressions.BoundRef.bindTo
import scraper.expressions.aggregates.AggregateFunction
import scraper.plans.physical.{PhysicalPlan, UnaryPhysicalPlan}

case class HashAggregate(
  child: PhysicalPlan,
  keyAliases: Seq[GroupingAlias],
  aggAliases: Seq[AggregationAlias]
) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = (keyAliases ++ aggAliases) map (_.toAttribute)

  private lazy val boundKeys: Seq[Expression] = keyAliases map (_.child) map bindTo(child.output)

  private lazy val bufferBuilder: AggregationBufferBuilder = {
    val boundAggs = aggAliases map (_.child) map bindTo(child.output)
    new AggregationBufferBuilder(boundAggs)
  }

  private lazy val hashMap = mutable.HashMap.empty[Row, AggregationBuffer]

  override def iterator: Iterator[Row] = {
    // Builds the hash map by consuming all input rows
    child.iterator foreach { input =>
      val groupingRow = Row.fromSeq(boundKeys map (_ evaluate input))
      hashMap.getOrElseUpdate(groupingRow, bufferBuilder.newBuffer()) += input
    }

    val aggResult = new BasicMutableRow(aggAliases.length)
    val joinedRow = new JoinedRow()

    hashMap.iterator map {
      case (groupingRow, aggBuffer) =>
        aggBuffer.result(aggResult)
        joinedRow(groupingRow, aggResult)
    }
  }
}

case class AggregationBuffer(boundFunctions: Seq[AggregateFunction], states: Seq[MutableRow]) {
  require(boundFunctions.length == states.length)

  (boundFunctions, states).zipped foreach (_ zero _)

  def +=(input: Row): Unit = (boundFunctions, states).zipped foreach (_.update(_, input))

  def result(mutableResult: MutableRow): Unit = mutableResult.indices foreach { i =>
    boundFunctions(i).result(mutableResult, i, states(i))
  }
}

class AggregationBufferBuilder(boundFunctions: Seq[AggregateFunction]) {
  private val (bufferLength, slicesBuilder) = {
    val lengths = boundFunctions map (_.stateSchema.length)
    val beginIndices = lengths.scan(0)(_ + _).init

    def slicesBuilder(row: MutableRow) = (beginIndices, lengths).zipped.map {
      new MutableRowSlice(row, _, _)
    }

    (lengths.sum, slicesBuilder _)
  }

  def newBuffer(): AggregationBuffer = {
    val mutableRow = new BasicMutableRow(bufferLength)
    AggregationBuffer(boundFunctions, slicesBuilder(mutableRow))
  }
}
