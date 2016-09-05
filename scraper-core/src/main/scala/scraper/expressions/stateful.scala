package scraper.expressions

import scala.util.Random

import scraper.Row
import scraper.expressions.typecheck.{Foldable, TypeConstraint}
import scraper.types.{DataType, DoubleType, LongType}

case class Rand(seed: Expression) extends UnaryExpression with StatefulExpression[Random] {
  override def dataType: DataType = DoubleType

  override def child: Expression = seed

  override protected def typeConstraint: TypeConstraint =
    seed sameTypeAs LongType andAlso Foldable

  private lazy val evaluatedSeed: Long = seed.evaluated.asInstanceOf[Long]

  override protected lazy val initialState: Random = new Random(evaluatedSeed)

  override protected def statefulEvaluate(state: Random, input: Row): (Any, Random) =
    (state.nextDouble(), state)

  override protected def template(childString: String): String = s"RAND($childString)"
}
