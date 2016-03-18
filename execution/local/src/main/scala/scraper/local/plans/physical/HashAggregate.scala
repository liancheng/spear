package scraper.local.plans.physical

import scala.collection.mutable

import scraper._
import scraper.expressions.BoundRef.bind
import scraper.expressions._
import scraper.plans.physical.{PhysicalPlan, UnaryPhysicalPlan}

case class HashAggregate(
  child: PhysicalPlan,
  keys: Seq[GroupingAlias],
  functions: Seq[AggregationAlias]
) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = (keys ++ functions) map (_.toAttribute)

  private lazy val boundKeys = keys map (_.child) map bind(child.output)

  private lazy val boundFunctions = functions map (_.child) map bind(child.output)

  private val bufferBuilder = new AggregationBufferBuilder(functions map (_.child))

  private val hashMap = mutable.HashMap.empty[Row, AggregationBuffer]

  override def iterator: Iterator[Row] = {
    child.iterator foreach { input =>
      val keyRow = Row.fromSeq(boundKeys map (_ evaluate input))
      val aggBuffer = hashMap.getOrElseUpdate(keyRow, {
        val buffer = bufferBuilder.newBuffer()
        (boundFunctions, buffer.slices).zipped foreach (_ zero _)
        buffer
      })

      (boundFunctions, aggBuffer.slices).zipped foreach (_.accumulate(_, input))
    }

    hashMap.iterator map {
      case (keyRow, valueBuffer) =>
        (boundFunctions, valueBuffer.slices).zipped foreach (_ result _)
        new JoinedRow(keyRow, valueBuffer.buffer)
    }
  }
}

case class AggregationBuffer(buffer: MutableRow, slices: Seq[MutableRow])

class AggregationBufferBuilder(functions: Seq[AggregateFunction]) {
  val (bufferLength, slicesBuilder) = {
    val lengths = functions map (_.bufferSchema.length)
    val begins = lengths.inits.toSeq.tail reverseMap (_.sum)
    val bufferLength = lengths.sum

    def slicesBuilder(row: MutableRow) =
      (begins, lengths).zipped map (new MutableRowSlice(row, _, _))

    (bufferLength, slicesBuilder _)
  }

  def newBuffer(): AggregationBuffer = {
    val mutableRow = new BasicMutableRow(bufferLength)
    AggregationBuffer(mutableRow, slicesBuilder(mutableRow))
  }
}
