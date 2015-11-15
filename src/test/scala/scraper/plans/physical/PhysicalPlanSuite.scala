package scraper.plans.physical

import scraper.expressions.{ Alias, Literal }
import scraper.types.TestUtils
import scraper.{ LoggingFunSuite, Row }

class PhysicalPlanSuite extends LoggingFunSuite with TestUtils {
  def checkPhysicalPlan(plan: PhysicalPlan, expected: Traversable[Row]): Unit = {
    assert(plan.iterator.toSeq === expected.toSeq)
  }

  test("project") {
    checkPhysicalPlan(
      Project(Alias("a", Literal(1)) :: Nil, SingleRowRelation),
      Seq(Row(1))
    )
  }
}
