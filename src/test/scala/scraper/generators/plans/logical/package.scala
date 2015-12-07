package scraper.generators.plans

import org.scalacheck.Gen

import scraper.Row
import scraper.config.Settings
import scraper.config.Settings.Key
import scraper.generators.expressions._
import scraper.generators.types._
import scraper.plans.logical.{Inner, Join, LocalRelation, LogicalPlan}

package object logical {
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
    from <- genFromClause(input)(settings)

    maybeSelect <- Gen option genSelectClause(from)(settings)
    withSelect = maybeSelect getOrElse from

    maybeWhere <- Gen option genWhereClause(withSelect)(settings)
    withWhere = maybeWhere getOrElse withSelect

    maybeLimit <- Gen option genLimitClause(withWhere)(settings)
    withLimit = maybeLimit getOrElse withWhere
  } yield withLimit

  def genFromClause(input: Seq[LogicalPlan])(implicit settings: Settings): Gen[LogicalPlan] = {
    val genRelations = if (input.nonEmpty) {
      Gen oneOf input
    } else {
      genTupleType(settings.withValue(AllowEmptyTupleType, false)).map {
        LocalRelation(Seq.empty[Row], _)
      }
    }

    for {
      firstRelation <- genRelations
      joinNum <- Gen choose (0, settings(MaxJoinNum))
      otherRelations <- Gen listOfN (joinNum, genRelations)
    } yield otherRelations.foldLeft(firstRelation: LogicalPlan) {
      Join(_, _, Inner, None)
    }
  }

  private lazy val genIdentifier: Gen[String] = for {
    length <- Gen choose (2, 4)
    prefix <- Gen.alphaLowerChar
    suffix <- Gen.listOfN(length - 1, Gen.numChar)
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
    Gen.resize(settings(MaxWherePredicateSize), genPredicate(plan.output)(settings)) map plan.filter

  def genLimitClause(plan: LogicalPlan)(implicit settings: Settings): Gen[LogicalPlan] =
    Gen choose (1, settings(MaxLimit)) map plan.limit
}
