package scraper.plans.physical

import scraper.expressions.dsl._
import scraper.{TestUtils, LoggingFunSuite, Row}

class PhysicalPlanSuite extends LoggingFunSuite with TestUtils {
  private val Seq(a1, b1) = Seq('a.int.!, 'b.string.?)

  private val r1 = LocalRelation(
    Seq(Row(1, "a"), Row(2, "b")),
    Seq(a1, b1)
  )

  private val Seq(a2, b2) = Seq('a.int.!, 'b.string.?)

  private val r2 = LocalRelation(
    Seq(Row(1, "a"), Row(3, "c")),
    Seq(a2, b2)
  )

  def checkPhysicalPlan(plan: PhysicalPlan, expected: Traversable[Row]): Unit = {
    assert(plan.iterator.toSeq === expected.toSeq)
  }

  def checkPhysicalPlan(plan: PhysicalPlan, first: Row, rest: Row*): Unit = {
    checkPhysicalPlan(plan, first +: rest)
  }

  test("project") {
    checkPhysicalPlan(
      SingleRowRelation select (1 as 'a),
      Row(1)
    )

    checkPhysicalPlan(
      r1 select a1 + 1,
      Row(2), Row(3)
    )
  }

  test("filter") {
    checkPhysicalPlan(
      r1 where a1 > 1,
      Row(2, "b")
    )
  }

  test("limit") {
    checkPhysicalPlan(
      r1 limit 1,
      Row(1, "a")
    )
  }

  test("sort") {
    checkPhysicalPlan(
      r1 orderBy a1.desc(true),
      Row(2, "b"), Row(1, "a")
    )
  }

  test("union") {
    checkPhysicalPlan(
      r1 union r2,
      Row(1, "a"),
      Row(2, "b"),
      Row(1, "a"),
      Row(3, "c")
    )
  }

  test("intersect") {
    checkPhysicalPlan(
      r1 intersect r2,
      Row(1, "a")
    )
  }

  test("except") {
    checkPhysicalPlan(
      r1 except r2,
      Row(2, "b")
    )
  }

  test("cartesian join") {
    checkPhysicalPlan(
      r1 cartesian r2,
      Row(1, "a", 1, "a"),
      Row(1, "a", 3, "c"),
      Row(2, "b", 1, "a"),
      Row(2, "b", 3, "c")
    )

    checkPhysicalPlan(
      r1 cartesian r2 on a1 =:= a2,
      Row(1, "a", 1, "a")
    )

    checkPhysicalPlan(
      r1 cartesian r2 on a1 > a2,
      Row(2, "b", 1, "a")
    )
  }
}
