package scraper.expressions.aggregates

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import scraper.{MutableRow, Name, Row}
import scraper.expressions.{Expression, UnaryExpression}
import scraper.types.{ArrayType, DataType, StructType}

abstract class Collect(child: Expression) extends AggregateFunction with UnaryExpression {
  override def isNullable: Boolean = false

  override def aggBufferSchema: StructType = StructType('collection -> dataType.!)

  override protected lazy val strictDataType: DataType = ArrayType(child.dataType, child.isNullable)
}

case class CollectList(child: Expression) extends Collect(child) {
  override def nodeName: Name = "collect_list"

  override def zero(aggBuffer: MutableRow): Unit = aggBuffer(0) = ArrayBuffer.empty[Any]

  override def update(aggBuffer: MutableRow, input: Row): Unit = {
    aggBuffer.head.asInstanceOf[ArrayBuffer[Any]] += child.evaluate(input)
  }

  override def merge(aggBuffer: MutableRow, inputAggBuffer: Row): Unit = {
    val from = inputAggBuffer.head.asInstanceOf[ArrayBuffer[Any]]
    val into = aggBuffer.head.asInstanceOf[ArrayBuffer[Any]]
    into ++= from
  }

  override def result(resultBuffer: MutableRow, ordinal: Int, aggBuffer: Row): Unit =
    resultBuffer(ordinal) = aggBuffer.head.asInstanceOf[ArrayBuffer[Any]]
}

case class CollectSet(child: Expression) extends Collect(child) {
  override def nodeName: Name = "collect_set"

  override def zero(aggBuffer: MutableRow): Unit = aggBuffer(0) = mutable.Set.empty[Any]

  override def update(aggBuffer: MutableRow, input: Row): Unit = {
    aggBuffer.head.asInstanceOf[mutable.Set[Any]] += child.evaluate(input)
  }

  override def merge(aggBuffer: MutableRow, inputAggBuffer: Row): Unit = {
    val from = inputAggBuffer.head.asInstanceOf[mutable.Set[Any]]
    val into = aggBuffer.head.asInstanceOf[mutable.Set[Any]]
    into ++= from
  }

  override def result(resultBuffer: MutableRow, ordinal: Int, aggBuffer: Row): Unit =
    resultBuffer(ordinal) = aggBuffer.head.asInstanceOf[mutable.Set[Any]].toSeq
}
