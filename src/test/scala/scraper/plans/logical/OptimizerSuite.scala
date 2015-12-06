package scraper.plans.logical

import scala.language.implicitConversions

import org.scalacheck.{Shrink, Arbitrary}
import org.scalacheck.Prop.forAll
import org.scalacheck.util.Pretty
import org.scalatest.prop.Checkers

import scraper.Test.defaultSettings
import scraper.expressions.Literal.True
import scraper.expressions.Predicate.splitConjunction
import scraper.expressions._
import scraper.generators.expressions._
import scraper.plans.Optimizer.{CNFConversion, ReduceFilters}
import scraper.trees.RulesExecutor.{EndCondition, FixedPoint}
import scraper.trees.{Rule, RulesExecutor}
import scraper.types.{FieldSpec, TestUtils, TupleType}
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
    implicit val arbPredicate = Arbitrary(genPredicate(TupleType.empty.toAttributes))

    check(
      forAll { predicate: Expression =>
        val optimizedPlan = optimizer(SingleRowRelation filter predicate)
        val conditions = optimizedPlan.collect {
          case _ Filter condition => splitConjunction(condition)
        }.flatten

        conditions.forall {
          // Within generated predicate expressions, there can be nested conjunctions within
          // comparison expressions, which should be ignore.  For example, the following predicate
          // is in CNF although the `=` comparison contains a nested conjunction:
          //
          //   (a > 1) AND ((TRUE AND FALSE) = FALSE)
          //
          // Here we simply replace them with a boolean literal.
          _ transformDown {
            case BinaryComparison(_ And _, _) => True
            case BinaryComparison(_, _ And _) => True
          } forall {
            case _ And _ => false
            case _       => true
          }
        }
      },

      // CNF conversion may potentially expand the predicate significantly and slows down the test
      // quite a bit.  Here we restrict the max size of the expression tree to avoid slow test runs.
      MaxSize(20)
    )
  }

  testRule(ReduceFilters, FixedPoint.Unlimited) { optimizer =>
    implicit val arbPredicate = Arbitrary(genPredicate(TupleType.empty.toAttributes))

    check { (p1: Expression, p2: Expression) =>
      val optimized = optimizer(SingleRowRelation filter p1 filter p2)
      val conditions = optimized.collect {
        case f: Filter => f.condition
      }

      conditions == Seq(p1 && p2)
    }
  }

  object WrongCNFConversion extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan: Filter =>
        plan transformExpressionsDown {
          case Not(lhs Or rhs)                => !lhs && !rhs
          case Not(lhs And rhs)               => !lhs || !rhs
          case (innerLhs And innerRhs) Or rhs => (innerLhs || rhs) && (innerRhs || rhs)
          // case lhs Or (innerLhs And innerRhs) => (innerLhs || lhs) && (innerRhs || lhs)
        }
    }
  }

  implicit val expressionShrink: Shrink[Expression] = Shrink { input =>
    if (input.children.length == 0) {
      // TODO: shrink leaf node
      Stream.empty
    } else {
      input.children.toStream :+ removeLeaf(input)
    }
  }

  private def removeLeaf(expr: Expression): Expression = {
    var stop = false
    expr transformUp {
      case e if !stop && e.children.length > 0 =>
        stop = true
        genLiteral(FieldSpec(e.dataType, e.nullable)).sample.get
    }
  }

  // This test case is just a copy of CNF test.
  // The WrongCNFConversion is obvious wrong when the right child of `Or` is `And`, let's see
  // if we can shrink the input into the minimal format that can fail test.
  testRule(WrongCNFConversion, FixedPoint.Unlimited) { optimizer =>
    implicit val arbPredicate = Arbitrary(genPredicate(TupleType.empty.toAttributes))

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
      MaxSize(20)
    )
  }

}
