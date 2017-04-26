package scraper.plans.logical

import scala.language.implicitConversions

import org.scalacheck.Arbitrary
import org.scalacheck.Prop.forAll
import org.scalacheck.util.Pretty
import org.scalatest.prop.Checkers

import scraper.{InMemoryCatalog, LoggingFunSuite, TestUtils}
import scraper.Test.defaultSettings
import scraper.exceptions.LogicalPlanUnresolvedException
import scraper.expressions._
import scraper.expressions.Predicate.splitConjunction
import scraper.expressions.functions._
import scraper.generators.expressions._
import scraper.plans.logical.Optimizer._
import scraper.plans.logical.analysis.Analyzer
import scraper.trees.{Rule, RulesExecutor}
import scraper.trees.RulesExecutor.{EndCondition, FixedPoint}
import scraper.types.{DoubleType, IntType, LongType}

class OptimizerSuite extends LoggingFunSuite with Checkers with TestUtils {
  private implicit def prettyExpression(expression: Expression): Pretty = Pretty {
    _ => "\n" + expression.prettyTree
  }

  private val analyzer = new Analyzer(new InMemoryCatalog)

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val relation = LocalRelation.empty(a, b)

  private def testRule(
    rule: Rule[LogicalPlan],
    endCondition: EndCondition,
    needsAnalyzer: Boolean = true
  )(
    f: (LogicalPlan => LogicalPlan) => Unit
  ): Unit = {
    test(rule.getClass.getSimpleName stripSuffix "$") {
      val optimizer = new RulesExecutor[LogicalPlan] {
        override def batches: Seq[RuleBatch] = Seq(
          RuleBatch("TestBatch", endCondition, rule :: Nil)
        )
      }

      if (needsAnalyzer) {
        f(analyzer andThen optimizer)
      } else {
        f(optimizer)
      }
    }
  }

  test("optimizer should reject unresolved logical plan") {
    intercept[LogicalPlanUnresolvedException] {
      (new Optimizer)(UnresolvedRelation('t)())
    }
  }

  testRule(CNFConversion, FixedPoint.Unlimited) { optimizer =>
    implicit val arbPredicate = Arbitrary(genLogicalPredicate(relation.output))

    check(
      forAll { predicate: Expression =>
        val optimizedPlan = optimizer(relation filter predicate)
        val conditions = optimizedPlan.collect { case f: Filter => f.condition }
        conditions flatMap splitConjunction forall (_.collect { case _: And => }.isEmpty)
      },

      // CNF conversion may potentially expand the predicate significantly and slows down the test
      // quite a bit.  Here we restrict the max size of the expression tree to avoid slow test runs.
      MaxSize(20)
    )
  }

  testRule(MergeFilters, FixedPoint.Unlimited) { optimizer =>
    implicit val arbPredicate = Arbitrary(genPredicate(relation.output))

    check { (p1: Expression, p2: Expression) =>
      val optimized = optimizer(relation filter p1 filter p2)
      val Seq(condition) = optimized.collect { case f: Filter => f.condition }
      condition == (p1 && p2)
    }
  }

  testRule(FoldNegation, FixedPoint.Unlimited) { optimizer =>
    implicit val arbPredicate = Arbitrary(genPredicate(relation.output))

    check { p: Expression =>
      val optimized = optimizer(relation filter p)
      val Seq(condition) = optimized.collect { case f: Filter => f.condition }
      condition.collectFirst { case _: Not => }.isEmpty
    }
  }

  testRule(PushFilterThroughAggregate, FixedPoint.Unlimited, needsAnalyzer = false) { optimizer =>
    val groupA = GroupingAlias(a)
    val aggCountB = AggregationAlias(count(b))

    checkPlan(
      optimizer(
        relation
          resolvedGroupBy groupA
          agg aggCountB
          filter groupA > 3
      ),

      relation
        filter groupA > 3
        resolvedGroupBy groupA
        agg aggCountB
    )

    checkPlan(
      optimizer(
        relation
          resolvedGroupBy groupA
          agg aggCountB
          filter aggCountB > 0
      ),

      relation
        resolvedGroupBy groupA
        agg aggCountB
        filter aggCountB > 0
    )

    checkPlan(
      optimizer(
        relation
          resolvedGroupBy groupA
          agg aggCountB
          filter groupA > 3 && aggCountB > 0
      ),

      relation
        filter groupA > 3
        resolvedGroupBy groupA
        agg aggCountB
        filter aggCountB > 0
    )
  }

  testRule(EliminateRedundantLimits, FixedPoint.Unlimited) { optimizer =>
    checkPlan(
      optimizer(relation limit 10 limit 3),
      relation limit Least(10, 3)
    )
  }

  testRule(PushProjectThroughLimit, FixedPoint.Unlimited) { optimizer =>
    checkPlan(
      optimizer(relation limit 1 select a),
      relation select a limit 1
    )
  }

  testRule(EliminateConstantFilter, FixedPoint.Unlimited) { optimizer =>
    checkPlan(
      optimizer(relation filter Literal.True),
      relation
    )

    checkPlan(
      optimizer(relation filter Literal.False),
      LocalRelation.empty(relation.output)
    )
  }

  testRule(PushFilterThroughJoin, FixedPoint.Unlimited) { optimizer =>
    val newRelation = relation.newInstance()
    val Seq(newA: AttributeRef, newB: AttributeRef) = newRelation.output

    checkPlan(
      optimizer(
        relation
          subquery 'x
          join (relation subquery 'y)
          filter $"x.a" === $"y.a" && $"x.a" > 0 && $"y.b".isNotNull
      ),

      relation
        subquery 'x
        filter (a of 'x) > 0
        join (newRelation subquery 'y filter (newB of 'y).isNotNull)
        on (a of 'x) === (newA of 'y)
    )
  }

  testRule(EliminateRedundantCast, FixedPoint.Unlimited) { optimizer =>
    checkPlan(
      optimizer(relation select (a cast IntType as 'x)),
      relation select (a as 'x)
    )

    checkPlan(
      optimizer(relation select (a cast LongType cast DoubleType as 'x)),
      relation select (a cast DoubleType as 'x)
    )
  }
}
