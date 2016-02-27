package scraper.expressions

import scala.language.higherKinds
import scala.util.Random
import scalaz.Applicative

import scraper.Row
import scraper.types.{DataType, DoubleType}

case class Rand(seed: Long) extends StatefulExpression[Random] with LeafExpression {
  override def dataType: DataType = DoubleType

  override protected lazy val initialState: Random = new Random(seed)

  override protected def statefulEvaluate(state: Random, input: Row): (Any, Random) =
    (state.nextDouble(), state)

  override protected def template[T[_]: Applicative](f: (Expression) => T[String]): T[String] =
    implicitly[Applicative[T]].point(s"RAND(${seed}L)")
}
