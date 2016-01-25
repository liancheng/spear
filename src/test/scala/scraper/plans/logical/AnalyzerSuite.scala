package scraper.plans.logical

import scraper.expressions.dsl._
import scraper.plans.logical.Analyzer.{ExpandStars, ResolveReferences, ResolveSelfJoins}
import scraper.trees.RulesExecutor.{EndCondition, FixedPoint}
import scraper.trees.{Rule, RulesExecutor}
import scraper.{LoggingFunSuite, TestUtils}

class AnalyzerSuite extends LoggingFunSuite with TestUtils {
  def singleBatchAnalyzer(
    endCondition: EndCondition,
    rules: Rule[LogicalPlan]*
  ): RulesExecutor[LogicalPlan] = {
    new RulesExecutor[LogicalPlan] {
      override def batches: Seq[RuleBatch] =
        RuleBatch("Testing batch", endCondition, rules) :: Nil
    }
  }

  test("resolve references") {
    val analyze = singleBatchAnalyzer(FixedPoint.Unlimited, ResolveReferences)
    val relation = LocalRelation.empty('a.int.!, 'b.string.?)

    checkPlan(
      analyze(relation select ('b, ('a + 1) as 's)),
      relation select ('b.string.?, ('a.int.! + 1) as 's)
    )
  }

  test("expand stars") {
    val analyze = singleBatchAnalyzer(FixedPoint.Unlimited, ExpandStars)
    val relation = LocalRelation.empty('a.int.!, 'b.string.?)

    checkPlan(
      analyze(relation select '*),
      relation select ('a.int.!, 'b.string.?)
    )
  }

  test("self-join not supported") {
    val analyze = singleBatchAnalyzer(FixedPoint.Unlimited, ResolveReferences, ResolveSelfJoins)
    val relation = LocalRelation.empty('a.int.!)

    intercept[UnsupportedOperationException] {
      analyze(relation join relation)
    }
  }
}
