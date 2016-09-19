package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.{AnalysisException, ResolutionFailureException}
import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.logical.LocalRelation

class PostAnalysisCheckSuite extends AnalyzerTest {
  test("post-analysis check - reject unresolved expressions") {
    val rule = new RejectUnresolvedExpressions(catalog)

    intercept[ResolutionFailureException] {
      rule(relation select 'a)
    }
  }

  test("post-analysis check - reject unresolved plans") {
    val rule = new RejectUnresolvedPlans(catalog)

    intercept[ResolutionFailureException] {
      rule(relation agg (1 as 'a))
    }
  }

  test("post-analysis check - reject top-level generated attributes") {
    val rule = new RejectTopLevelGeneratedAttributes(catalog)

    intercept[ResolutionFailureException] {
      rule(LocalRelation.empty(GroupingAlias('a.int.!).attr))
    }
  }

  test("post-analysis check - reject distinct aggregate functions") {
    val rule = new RejectDistinctAggregateFunctions(catalog)

    intercept[ResolutionFailureException] {
      rule(relation select distinct(count(a)))
    }
  }

  test("post-analysis check - reject orphan attribute references") {
    val rule = new RejectOrphanAttributeReferences(catalog)

    intercept[AnalysisException] {
      rule(relation select 'c.int.!)
    }
  }

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val relation = LocalRelation.empty(a, b)
}
