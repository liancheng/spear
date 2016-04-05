package scraper.local.plans.physical

import scala.collection.mutable

import scraper._
import scraper.expressions._
import scraper.expressions.BoundRef.bind
import scraper.plans.physical.{PhysicalPlan, UnaryPhysicalPlan}

case class HashAggregate(
  child: PhysicalPlan,
  keys: Seq[GroupingAlias],
  functions: Seq[AggregationAlias]
) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = (keys ++ functions) map (_.toAttribute)

  private lazy val boundKeys = keys map (_.child) map bind(child.output)

  private lazy val boundFunctions = functions map (_.child) map bind(child.output)

  private val bufferBuilder = new AggregationBufferBuilder(boundFunctions)

  private val hashMap = mutable.HashMap.empty[Row, AggregationBuffer]

  override def iterator: Iterator[Row] = {
    child.iterator foreach { input =>
      val groupingRow = Row.fromSeq(boundKeys map (_ evaluate input))
      hashMap.getOrElseUpdate(groupingRow, bufferBuilder.newBuffer()) += input
    }

    val aggResult = new BasicMutableRow(functions.length)
    val joinedRow = new JoinedRow()

    hashMap.iterator map {
      case (groupingRow, aggBuffer) =>
        aggBuffer.result(aggResult)
        joinedRow(groupingRow, aggResult)
    }
  }
}

case class AggregationBuffer(boundFunctions: Seq[AggregateFunction], slices: Seq[MutableRow]) {
  (boundFunctions, slices).zipped foreach (_ zero _)

  def +=(row: Row): Unit = (boundFunctions, slices).zipped foreach (_.update(_, row))

  def ++=(other: AggregationBuffer): Unit =
    (boundFunctions, slices, other.slices).zipped foreach (_.merge(_, _))

  def result(mutableResult: MutableRow): Unit = mutableResult.indices foreach { i =>
    mutableResult(i) = boundFunctions(i) result slices(i)
  }
}

class AggregationBufferBuilder(boundFunctions: Seq[AggregateFunction]) {
  private val (bufferLength, slicesBuilder) = {
    val lengths = boundFunctions map (_.bufferSchema.length)
    val begins = lengths.inits.toSeq.tail reverseMap (_.sum)
    val bufferLength = lengths.sum

    def slicesBuilder(row: MutableRow) = (begins, lengths).zipped.map {
      new MutableRowSlice(row, _, _)
    }

    (bufferLength, slicesBuilder _)
  }

  def newBuffer(): AggregationBuffer = {
    val mutableRow = new BasicMutableRow(bufferLength)
    AggregationBuffer(boundFunctions, slicesBuilder(mutableRow))
  }
}
