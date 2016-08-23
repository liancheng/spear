package scraper.expressions.aggregates

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import scraper.{MutableRow, Name, Row}
import scraper.expressions.{Expression, UnaryExpression}
import scraper.types.{ArrayType, DataType, StructType}

abstract class Collect(child: Expression) extends AggregateFunction with UnaryExpression {
  override def isNullable: Boolean = false

  override def stateSchema: StructType = StructType('collection -> dataType.!)

  override protected lazy val strictDataType: DataType = ArrayType(child.dataType, child.isNullable)
}

case class CollectList(child: Expression) extends Collect(child) {
  override def nodeName: Name = "collect_list"

  override def zero(state: MutableRow): Unit = state(0) = ArrayBuffer.empty[Any]

  override def update(state: MutableRow, input: Row): Unit = {
    state.head.asInstanceOf[ArrayBuffer[Any]] += child.evaluate(input)
  }

  override def merge(state: MutableRow, inputState: Row): Unit = {
    val from = inputState.head.asInstanceOf[ArrayBuffer[Any]]
    val into = state.head.asInstanceOf[ArrayBuffer[Any]]
    into ++= from
  }

  override def result(resultBuffer: MutableRow, ordinal: Int, state: Row): Unit =
    resultBuffer(ordinal) = state.head.asInstanceOf[ArrayBuffer[Any]]
}

case class CollectSet(child: Expression) extends Collect(child) {
  override def nodeName: Name = "collect_set"

  override def zero(state: MutableRow): Unit = state(0) = mutable.Set.empty[Any]

  override def update(state: MutableRow, input: Row): Unit = {
    state.head.asInstanceOf[mutable.Set[Any]] += child.evaluate(input)
  }

  override def merge(state: MutableRow, inputState: Row): Unit = {
    val from = inputState.head.asInstanceOf[mutable.Set[Any]]
    val into = state.head.asInstanceOf[mutable.Set[Any]]
    into ++= from
  }

  override def result(resultBuffer: MutableRow, ordinal: Int, state: Row): Unit =
    resultBuffer(ordinal) = state.head.asInstanceOf[mutable.Set[Any]].toSeq
}
