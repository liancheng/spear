package scraper.plans.logical

import scala.language.implicitConversions

import org.scalacheck.Arbitrary
import org.scalacheck.Prop.forAll
import org.scalacheck.util.Pretty
import org.scalatest.prop.Checkers

import scraper.Test.defaultSettings
import scraper.expressions.Literal.True
import scraper.expressions.Predicate.splitConjunction
import scraper.expressions._
import scraper.generators.expressions._
import scraper.plans.Optimizer.{CNFConversion, ReduceFilters}
import scraper.plans.logical.OptimizerSuite.BadCNFConversion
import scraper.trees.RulesExecutor.{EndCondition, FixedPoint}
import scraper.trees.{Rule, RulesExecutor}
import scraper.types.{TestUtils, StructType}
import scraper.{Analyzer, LocalCatalog, LoggingFunSuite}

class OptimizerSuite extends LoggingFunSuite with Checkers with TestUtils {
  private implicit def prettyExpression(expression: Expression): Pretty = Pretty {
    _ => "\n" + expression.prettyTree
  }

  private def testRule(
    rule: Rule[LogicalPlan], endCondition: EndCondition
  )(
    f: (LogicalPlan => LogicalPlan) => Unit
  ): Unit = {
    test(rule.getClass.getSimpleName stripSuffix "$") {
      val analyzer = new Analyzer(new LocalCatalog)
      val optimizer = new RulesExecutor[LogicalPlan] {
        override def batches: Seq[RuleBatch] = Seq(
          RuleBatch("TestBatch", endCondition, rule :: Nil)
        )
      }

      f(analyzer andThen optimizer)
    }
  }

  testRule(CNFConversion, FixedPoint.Unlimited) { optimizer =>
    implicit val arbPredicate = Arbitrary(
      genPredicate(StructType.empty.toAttributes)(
        // Avoids generating nested conjunctions within other expressions that are not logical
        // operators to simplify the property defined below.
        defaultSettings.withValue(OnlyLogicalOperatorsInPredicate, true)
      )
    )

    check(
      forAll { predicate: Expression =>
        val optimizedPlan = optimizer(SingleRowRelation filter predicate)
        val conditions = optimizedPlan.collect {
          case _ Filter condition => splitConjunction(condition)
        }.flatten

        conditions.forall {
          _.collect { case _: And => () }.isEmpty
        }
      },

      // CNF conversion may potentially expand the predicate significantly and slows down the test
      // quite a bit.  Here we restrict the max size of the expression tree to avoid slow test runs.
      MaxSize(20)
    )
  }

  testRule(ReduceFilters, FixedPoint.Unlimited) { optimizer =>
    implicit val arbPredicate = Arbitrary(genPredicate(StructType.empty.toAttributes))

    check { (p1: Expression, p2: Expression) =>
      val optimized = optimizer(SingleRowRelation filter p1 filter p2)
      val conditions = optimized.collect {
        case f: Filter => f.condition
      }

      conditions == Seq(p1 && p2)
    }
  }

  testRule(BadCNFConversion, FixedPoint.Unlimited) { optimizer =>
    implicit val arbPredicate = Arbitrary(genPredicate(StructType.empty.toAttributes))

    check(
      forAll { predicate: Expression =>
        val optimizedPlan = optimizer(SingleRowRelation filter predicate)
        val conditions = optimizedPlan.collect {
          case _ Filter condition => splitConjunction(condition)
        }.flatten

        conditions.forall {
          _ transformDown {
            case BinaryComparison(_ And _, _) => True
            case BinaryComparison(_, _ And _) => True
          } forall {
            case _ And _ => false
            case _       => true
          }
        }
      },

      MaxSize(100),
      MinSize(100)
    )
  }
}

object OptimizerSuite {
  // This is a copy of the original CNFConversion rule with one match case removed.  So that it
  // doesn't properly handle cases where the right branch of an `Or` is an `And`.  It's used for
  // testing expression minimization (`shrinkExpression`).
  object BadCNFConversion extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan: Filter =>
        plan transformExpressionsDown {
          case Not(lhs Or rhs)                => !lhs && !rhs
          case Not(lhs And rhs)               => !lhs || !rhs
          case (innerLhs And innerRhs) Or rhs => (innerLhs || rhs) && (innerRhs || rhs)
        }
    }
  }
}
