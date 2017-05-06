package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.{AnalysisException, ResolutionFailureException}
import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.logical.LocalRelation

class PostAnalysisCheckSuite extends AnalyzerTest {
  test("post-analysis check - reject unresolved expressions") {
    val rule = new RejectUnresolvedExpression(catalog)

    intercept[ResolutionFailureException] {
      rule(relation select 'a)
    }
  }

  test("post-analysis check - reject unresolved plans") {
    val rule = new RejectUnresolvedPlan(catalog)

    intercept[ResolutionFailureException] {
      rule(relation `GROUP BY` Nil agg (1 as 'a))
    }
  }

  test("post-analysis check - reject top-level `InternalAttribute`s") {
    val rule = new RejectTopLevelInternalAttribute(catalog)

    intercept[ResolutionFailureException] {
      rule(LocalRelation.empty(GroupingAlias('a.int.!).attr))
    }
  }

  test("post-analysis check - reject distinct aggregate functions") {
    val rule = new RejectDistinctAggregateFunction(catalog)

    intercept[ResolutionFailureException] {
      rule(relation select distinct(count(a)))
    }
  }

  test("post-analysis check - reject orphan attribute references") {
    val rule = new RejectOrphanAttributeReference(catalog)

    intercept[AnalysisException] {
      rule(relation select 'c.int.!)
    }
  }

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val relation = LocalRelation.empty(a, b)
}
