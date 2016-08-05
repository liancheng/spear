package scraper.local.plans.physical

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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

  // Bound grouping key expressions
  private lazy val boundKeys: Seq[Expression] = keys map (_.child) map bind(child.output)

  private lazy val bufferBuilder: AggregationBufferBuilder = {
    // Bound aggregate function expressions
    val boundFunctions = functions map (_.child) map bind(child.output)
    new AggregationBufferBuilder(boundFunctions)
  }

  private lazy val hashMap = mutable.HashMap.empty[Row, AggregationBuffer]

  override def iterator: Iterator[Row] = {
    // Builds the hash map by consuming all input rows
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

  def +=(input: Row): Unit = (boundFunctions, slices).zipped foreach (_.update(_, input))

  def result(mutableResult: MutableRow): Unit = mutableResult.indices foreach { i =>
    boundFunctions(i).result(mutableResult, i, slices(i))
  }
}

class AggregationBufferBuilder(boundFunctions: Seq[AggregateFunction]) {
  private val (bufferLength, slicesBuilder) = {
    val lengths = boundFunctions map (_.aggBufferSchema.length)

    val beginIndices = ArrayBuffer.empty[Int]
    if (lengths.nonEmpty) {
      beginIndices += 0
      lengths.init.foreach(beginIndices += beginIndices.last + _)
    }

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
