package scraper.expressions.aggregates

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import scraper.{Name, Row}
import scraper.expressions.{Expression, UnaryExpression}
import scraper.types.{ArrayType, DataType}

case class CollectList(child: Expression)
  extends ImperativeAggregateFunction[ArrayBuffer[Any]]
  with UnaryExpression {

  override def isNullable: Boolean = false

  override protected lazy val strictDataType: DataType = ArrayType(child.dataType, child.isNullable)

  override def nodeName: Name = "collect_list"

  override protected val evaluator: Evaluator = new Evaluator {
    override def initialState: State = ArrayBuffer.empty[Any]

    override def update(state: State, input: Row): State = state += child.evaluate(input)

    override def merge(state: State, inputState: State): State = state ++= inputState

    override def result(state: State): Any = state
  }
}

case class CollectSet(child: Expression)
  extends ImperativeAggregateFunction[mutable.Set[Any]]
  with UnaryExpression {

  override def nodeName: Name = "collect_set"

  override protected val evaluator: Evaluator = new Evaluator {
    override def initialState: State = mutable.Set.empty[Any]

    override def update(state: State, input: Row): State = state += child.evaluate(input)

    override def merge(state: State, inputState: State): State = state ++= inputState

    override def result(state: State): Any = state.toSeq
  }
}
