package scraper.plans.physical

import scraper.Row
import scraper.expressions.{ Alias, Literal }
import scraper.types.TestUtils

class PhysicalPlanSuite extends TestUtils {
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
