package scraper.generators.plans

import org.scalacheck.Gen

import scraper.config.Settings
import scraper.generators.Keys._
import scraper.generators._
import scraper.generators.expressions._
import scraper.plans.logical._
import scraper.plans.logical.dsl._

package object logical {
  def genLogicalPlan(input: Seq[LogicalPlan])(implicit settings: Settings): Gen[LogicalPlan] = for {
    size <- Gen.size
    from <- genFromClause(input)(settings)

    maybeSelect <- Gen.resize(
      size - from.size,
      chanceOption(settings(SelectClauseChance), genSelectClause(from)(settings))
    )
    withSelect = maybeSelect getOrElse from

    maybeWhere <- Gen.resize(
      size - withSelect.size,
      chanceOption(settings(WhereClauseChance), genWhereClause(withSelect)(settings))
    )
    withWhere = maybeWhere getOrElse withSelect

    maybeLimit <- Gen.resize(
      size - withWhere.size,
      chanceOption(settings(LimitClauseChance), genLimitClause(withWhere)(settings))
    )
    withLimit = maybeLimit getOrElse withWhere
  } yield withLimit

  def genFromClause(input: Seq[LogicalPlan])(implicit settings: Settings): Gen[LogicalPlan] = for {
    size <- Gen.size
    joinNum <- Gen.choose(0, settings(MaxJoinNum))
    relationFactorSize = size / (joinNum + 1)

    head :: tails <- Gen.listOfN(
      joinNum + 1,
      Gen.resize(relationFactorSize, genRelationFactor(input)(settings))
    )
  } yield (tails foldLeft head) {
    Join(_, _, Inner, None)
  }

  def genRelationFactor(input: Seq[LogicalPlan])(implicit settings: Settings): Gen[LogicalPlan] = {
    val genBottomPlans = Gen.oneOf(input :+ SingleRowRelation)

    Gen.sized {
      case size if size < 2 =>
        genBottomPlans

      case size =>
        val genSubquery = for {
          plan <- Gen.resize(size - 1, Gen.lzy(genLogicalPlan(input)(settings)))
          name <- genIdentifier
        } yield Subquery(plan, name)

        Gen.oneOf(genSubquery, genBottomPlans)
    }
  }

  private lazy val genIdentifier: Gen[String] = for {
    length <- Gen.choose(2, 4)
    prefix <- Gen.alphaLowerChar
    suffix <- Gen listOfN (length - 1, Gen.numChar)
  } yield (prefix +: suffix).mkString

  def genSelectClause(plan: LogicalPlan)(implicit settings: Settings): Gen[LogicalPlan] = for {
    width <- Gen.choose(1, settings(MaxProjectWidth))
    aliases <- Gen.listOfN(width, genIdentifier) retryUntil (_.distinct.length == width)
    expressions <- Gen.listOfN(width, Gen.resize(
      settings(MaxSelectExpressionSize),
      genExpression(plan.output)(settings)
    ))

    projectList = (expressions, aliases).zipped map (_ as _)
  } yield plan select projectList

  def genWhereClause(plan: LogicalPlan)(implicit settings: Settings): Gen[LogicalPlan] =
    Gen.resize(
      settings(MaxWherePredicateSize),
      genPredicate(plan.output)(settings)
    ) map plan.filter

  def genLimitClause(plan: LogicalPlan)(implicit settings: Settings): Gen[LogicalPlan] =
    Gen.choose(1, settings(MaxLimit)) map plan.limit
}
