package scraper.generators.plans

import org.scalacheck.Gen

import scraper.config.Settings
import scraper.config.Settings.Key
import scraper.generators._
import scraper.generators.expressions._
import scraper.plans.logical._

package object logical {
  val SelectClauseChance: Key[Double] =
    Key("scraper.test.plans.chances.select-clause").double

  val FromClauseChance: Key[Double] =
    Key("scraper.test.plans.chances.from-clause").double

  val WhereClauseChance: Key[Double] =
    Key("scraper.test.plans.chances.where-clause").double

  val LimitClauseChance: Key[Double] =
    Key("scraper.test.plans.chances.limit-clause").double

  val MaxJoinNum: Key[Int] =
    Key("scraper.test.plans.max-join-num").int

  val MaxProjectWidth: Key[Int] =
    Key("scraper.test.plans.max-project-width").int

  val MaxLimit: Key[Int] =
    Key("scraper.test.plans.max-limit").int

  val MaxExpressionSize: Key[Int] =
    Key("scraper.test.plans.max-expression-size").int

  val MaxWherePredicateSize: Key[Int] =
    Key("scraper.test.plans.max-where-predicate-size").int

  val MaxSelectExpressionSize: Key[Int] =
    Key("scraper.test.plans.max-select-expression-size").int

  def genLogicalPlan(input: Seq[LogicalPlan])(implicit settings: Settings): Gen[LogicalPlan] = for {
    size <- Gen.size
    from <- genFromClause(input)(settings)

    maybeSelect <- Gen resize (
      size - from.size,
      chanceOption(settings(SelectClauseChance), genSelectClause(from)(settings))
    )
    withSelect = maybeSelect getOrElse from

    maybeWhere <- Gen resize (
      size - withSelect.size,
      chanceOption(settings(WhereClauseChance), genWhereClause(withSelect)(settings))
    )
    withWhere = maybeWhere getOrElse withSelect

    maybeLimit <- Gen resize (
      size - withWhere.size,
      chanceOption(settings(LimitClauseChance), genLimitClause(withWhere)(settings))
    )
    withLimit = maybeLimit getOrElse withWhere
  } yield withLimit

  def genFromClause(input: Seq[LogicalPlan])(implicit settings: Settings): Gen[LogicalPlan] = for {
    size <- Gen.size
    joinNum <- Gen choose (0, settings(MaxJoinNum))
    relationFactorSize = size / (joinNum + 1)

    head :: tails <- Gen listOfN (
      joinNum + 1,
      Gen resize (relationFactorSize, genRelationFactor(input)(settings))
    )
  } yield (tails foldLeft head) {
    Join(_, _, Inner, None)
  }

  def genRelationFactor(input: Seq[LogicalPlan])(implicit settings: Settings): Gen[LogicalPlan] = {
    val genBottomPlans = Gen oneOf (input :+ SingleRowRelation)

    Gen.sized {
      case size if size < 2 =>
        genBottomPlans

      case size =>
        val genSubquery = for {
          plan <- Gen resize (size - 1, Gen lzy genLogicalPlan(input)(settings))
          name <- genIdentifier
        } yield Subquery(plan, name)

        Gen oneOf (genSubquery, genBottomPlans)
    }
  }

  private lazy val genIdentifier: Gen[String] = for {
    length <- Gen choose (2, 4)
    prefix <- Gen.alphaLowerChar
    suffix <- Gen listOfN (length - 1, Gen.numChar)
  } yield (prefix +: suffix).mkString

  def genSelectClause(plan: LogicalPlan)(implicit settings: Settings): Gen[LogicalPlan] = for {
    width <- Gen choose (1, settings(MaxProjectWidth))
    aliases <- Gen listOfN (width, genIdentifier) retryUntil (_.distinct.length == width)
    expressions <- Gen listOfN (width, Gen resize (
      settings(MaxSelectExpressionSize),
      genExpression(plan.output)(settings)
    ))

    projections = (expressions, aliases).zipped map (_ as _)

  } yield plan select projections

  def genWhereClause(plan: LogicalPlan)(implicit settings: Settings): Gen[LogicalPlan] =
    Gen resize (settings(MaxWherePredicateSize), genPredicate(plan.output)(settings)) map plan.filter

  def genLimitClause(plan: LogicalPlan)(implicit settings: Settings): Gen[LogicalPlan] =
    Gen choose (1, settings(MaxLimit)) map plan.limit
}
