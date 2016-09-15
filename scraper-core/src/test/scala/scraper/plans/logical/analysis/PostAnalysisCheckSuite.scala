package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.ResolutionFailureException
import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.logical.LocalRelation

class PostAnalysisCheckSuite extends AnalyzerTest {
  test("post-analysis check - reject unresolved expressions") {
    val rule = new RejectUnresolvedExpressions(catalog)

    intercept[ResolutionFailureException] {
      rule(relation0 select 'a)
    }
  }

  test("post-analysis check - reject unresolved plans") {
    val rule = new RejectUnresolvedPlans(catalog)

    intercept[ResolutionFailureException] {
      rule(relation0 agg (1 as 'a))
    }
  }

  test("post-analysis check - reject generated attributes") {
    val rule = new RejectGeneratedAttributes(catalog)

    intercept[ResolutionFailureException] {
      rule(LocalRelation.empty(GroupingAlias('a.int.!).attr))
    }
  }

  test("post-analysis check - reject distinct aggregate function") {
    val rule = new RejectDistinctAggregateFunctions(catalog)

    intercept[ResolutionFailureException] {
      rule(relation0 select distinct(count(a)))
    }
  }

  private val (a, b) = ('a.int.!, 'b.string.?)

  private val relation0 = LocalRelation.empty(a, b)
}
