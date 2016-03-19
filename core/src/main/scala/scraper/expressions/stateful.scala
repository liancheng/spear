package scraper.expressions

import scraper.Row
import scraper.types.{DataType, DoubleType}

import scala.language.higherKinds
import scala.util.Random

case class Rand(seed: Long) extends StatefulExpression[Random] with LeafExpression {
  override def dataType: DataType = DoubleType

  override protected lazy val initialState: Random = new Random(seed)

  override protected def statefulEvaluate(state: Random, input: Row): (Any, Random) =
    (state.nextDouble(), state)

  override protected def template: String = s"RAND(${seed}L)"
}
