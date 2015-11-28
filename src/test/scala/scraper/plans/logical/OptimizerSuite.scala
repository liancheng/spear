package scraper.plans.logical

import org.scalacheck.Arbitrary
import org.scalacheck.Prop.forAll
import org.scalatest.prop.Checkers
import scraper.LoggingFunSuite
import scraper.Test.defaultSettings
import scraper.expressions.Predicate.splitConjunction
import scraper.expressions._
import scraper.generators.expressions._
import scraper.plans.Optimizer.{CNFConversion, CombineFilters}
import scraper.trees.{Rule, RulesExecutor}
import scraper.types.{TestUtils, TupleType}

class OptimizerSuite extends LoggingFunSuite with Checkers with TestUtils {
  private def makeOptimizer(rule: Rule[LogicalPlan]): RulesExecutor[LogicalPlan] =
    new RulesExecutor[LogicalPlan] {
      override def batches: Seq[RuleBatch] = Seq(
        RuleBatch("TestBatch", FixedPoint.Unlimited, rule :: Nil)
      )
    }

  ignore("CNFConversion") {
    val optimizer = makeOptimizer(CNFConversion)

    implicit val arbPredicate = Arbitrary(genPredicate(TupleType.empty.toAttributes))

    check { predicate: Expression =>
      val optimizedPlan = optimizer(SingleRowRelation filter predicate)
      val conditions = optimizedPlan.collect {
        case _ Filter condition => splitConjunction(condition)
      }.flatten

      conditions.forall {
        _.collect {
          case and: And => and
        }.isEmpty
      }
    }
  }

  test("CombineFilters") {
    val optimizer = makeOptimizer(CombineFilters)

    implicit val arbPredicate = Arbitrary(genPredicate(TupleType.empty.toAttributes))

    check { (condition1: Expression, condition2: Expression) =>
      val optimized = optimizer(SingleRowRelation filter condition1 filter condition2)
      val conditions = optimized.collect {
        case f: Filter => f.condition
      }

      assert(conditions.length === 1)
      conditions.head == (condition1 && condition2)
    }
  }
}
