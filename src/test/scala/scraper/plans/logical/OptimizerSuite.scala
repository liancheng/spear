package scraper.plans.logical

import org.scalacheck.Arbitrary
import org.scalatest.prop.Checkers
import scraper.Test.defaultSettings
import scraper.expressions.Predicate.splitConjunction
import scraper.expressions._
import scraper.generators.expressions._
import scraper.plans.Optimizer.{CNFConversion, ReduceFilters}
import scraper.trees.{Rule, RulesExecutor}
import scraper.types.{BooleanType, TestUtils, TupleType}
import scraper.{Analyzer, LocalCatalog, LoggingFunSuite}

class OptimizerSuite extends LoggingFunSuite with Checkers with TestUtils {
  private def testRule(rule: Rule[LogicalPlan])(f: (LogicalPlan => LogicalPlan) => Unit): Unit = {
    test(rule.getClass.getSimpleName) {
      val optimizer = new Analyzer(new LocalCatalog) andThen new RulesExecutor[LogicalPlan] {
        override def batches: Seq[RuleBatch] = Seq(
          RuleBatch("TestBatch", FixedPoint.Unlimited, rule :: Nil)
        )
      }

      f(optimizer)
    }
  }

  testRule(CNFConversion) { optimizer =>
    implicit val arbPredicate = Arbitrary(genPredicate(TupleType.empty.toAttributes, BooleanType))

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

  testRule(ReduceFilters) { optimizer =>
    implicit val arbPredicate = Arbitrary(genPredicate(TupleType.empty.toAttributes, BooleanType))

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
