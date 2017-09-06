package spear.plans.logical

import scala.language.implicitConversions

import org.scalacheck.Arbitrary
import org.scalacheck.Prop.forAll
import org.scalacheck.util.Pretty
import org.scalatest.prop.Checkers

import spear.{InMemoryCatalog, LoggingFunSuite, TestUtils}
import spear.Test.defaultSettings
import spear.exceptions.LogicalPlanUnresolvedException
import spear.expressions._
import spear.expressions.Predicate.splitConjunction
import spear.expressions.functions._
import spear.generators.expressions._
import spear.plans.logical.analysis.Analyzer
import spear.trees._
import spear.types.{DoubleType, IntType, LongType}

class OptimizerSuite extends LoggingFunSuite with Checkers with TestUtils {
  private implicit def prettyExpression(expression: Expression): Pretty = Pretty {
    _ => "\n" + expression.prettyTree
  }

  private val analyzer = new Analyzer(new InMemoryCatalog)

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val relation = LocalRelation.empty(a, b)

  private def testRule(
    rule: Rule[LogicalPlan],
    convergenceTest: ConvergenceTest,
    needsAnalyzer: Boolean = true
  )(
    f: (LogicalPlan => LogicalPlan) => Unit
  ): Unit = {
    test(rule.getClass.getSimpleName stripSuffix "$") {
      val optimizer = new Transformer(RuleGroup(convergenceTest, rule :: Nil))

      if (needsAnalyzer) {
        f(analyzer andThen optimizer)
      } else {
        f(optimizer)
      }
    }
  }

  test("optimizer should reject unresolved logical plan") {
    intercept[LogicalPlanUnresolvedException] {
      (new Optimizer)(UnresolvedRelation('t))
    }
  }

  testRule(CNFConversion, FixedPoint) { optimizer =>
    implicit val arbPredicate: Arbitrary[Expression] =
      Arbitrary(genLogicalPredicate(relation.output))

    check(
      forAll { predicate: Expression =>
        val optimizedPlan = optimizer(relation filter predicate)
        val conditions = optimizedPlan.collectDown { case f: Filter => f.condition }
        conditions flatMap splitConjunction forall (_.collectDown { case _: And => }.isEmpty)
      },

      // CNF conversion may potentially expand the predicate significantly and slows down the test
      // quite a bit.  Here we restrict the max size of the expression tree to avoid slow test runs.
      MaxSize(20)
    )
  }

  testRule(MergeFilters, FixedPoint) { optimizer =>
    implicit val arbPredicate: Arbitrary[Expression] = Arbitrary(genPredicate(relation.output))

    check { (p1: Expression, p2: Expression) =>
      val optimized = optimizer(relation filter p1 filter p2)
      val Seq(condition) = optimized.collectDown { case f: Filter => f.condition }
      condition == (p1 && p2)
    }
  }

  testRule(FoldNegation, FixedPoint) { optimizer =>
    implicit val arbPredicate: Arbitrary[Expression] = Arbitrary(genPredicate(relation.output))

    check { p: Expression =>
      val optimized = optimizer(relation filter p)
      val Seq(condition) = optimized.collectDown { case f: Filter => f.condition }
      condition.collectFirstDown { case _: Not => }.isEmpty
    }
  }

  testRule(PushFilterThroughAggregate, FixedPoint, needsAnalyzer = false) { optimizer =>
    val groupA = GroupingKeyAlias(a)
    val aggCountB = AggregateFunctionAlias(count(b))

    checkPlan(
      optimizer(
        relation
          aggregate (groupA :: Nil, aggCountB :: Nil)
          filter groupA > 3
      ),

      relation
        filter groupA > 3
        aggregate (groupA :: Nil, aggCountB :: Nil)
    )

    checkPlan(
      optimizer(
        relation
          aggregate (groupA :: Nil, aggCountB :: Nil)
          filter aggCountB > 0
      ),

      relation
        aggregate (groupA :: Nil, aggCountB :: Nil)
        filter aggCountB > 0
    )

    checkPlan(
      optimizer(
        relation
          aggregate (groupA :: Nil, aggCountB :: Nil)
          filter groupA > 3 && aggCountB > 0
      ),

      relation
        filter groupA > 3
        aggregate (groupA :: Nil, aggCountB :: Nil)
        filter aggCountB > 0
    )
  }

  testRule(EliminateRedundantLimits, FixedPoint) { optimizer =>
    checkPlan(
      optimizer(relation limit 10 limit 3),
      relation limit Least(10, 3)
    )
  }

  testRule(PushProjectThroughLimit, FixedPoint) { optimizer =>
    checkPlan(
      optimizer(relation limit 1 select a),
      relation select a limit 1
    )
  }

  testRule(EliminateConstantFilter, FixedPoint) { optimizer =>
    checkPlan(
      optimizer(relation filter Literal.True),
      relation
    )

    checkPlan(
      optimizer(relation filter Literal.False),
      LocalRelation.empty(relation.output)
    )
  }

  testRule(PushFilterThroughJoin, FixedPoint) { optimizer =>
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

  testRule(EliminateRedundantCast, FixedPoint) { optimizer =>
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
