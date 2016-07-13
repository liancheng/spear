package scraper.local.plans.physical

import scraper.{LoggingFunSuite, Row, TestUtils}
import scraper.expressions._
import scraper.local.plans.physical.dsl._
import scraper.plans.physical.{PhysicalPlan, SingleRowRelation}

class LocalPhysicalPlanSuite extends LoggingFunSuite with TestUtils {
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

  private val Seq(a3, b3) = Seq('a.int.!, 'b.string.?)

  private val r3 = LocalRelation(
    Seq(
      Row(1: Integer, "a"),
      Row(3: Integer, "c"),
      Row(null: Integer, "b"),
      Row(4: Integer, null)
    ),
    Seq(a3, b3)
  )

  def checkPhysicalPlan(plan: PhysicalPlan, expectedRows: Traversable[Row]): Unit = {
    val planOrdered = plan.collectFirst { case _: Sort => }.nonEmpty

    if (planOrdered) {
      assert(plan.iterator.toSeq == expectedRows.toSeq)
    } else {
      val sort = Sort(_: PhysicalPlan, plan.output map (_.asc))
      val actual = sort(plan).iterator.toSeq
      val expected = sort(LocalRelation(expectedRows.toIterable, plan.output)).iterator.toSeq
      assert(actual == expected)
    }
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
      r1 select (a1 + 1 as 'f),
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
      r1 limit 0,
      Nil
    )

    checkPhysicalPlan(
      r1 limit 1,
      Row(1, "a")
    )

    checkPhysicalPlan(
      r1 limit 3,
      Row(1, "a"),
      Row(2, "b")
    )
  }

  test("sort") {
    checkPhysicalPlan(
      r1 orderBy a1.desc,
      Row(2, "b"), Row(1, "a")
    )

    checkPhysicalPlan(
      r1 orderBy a1.asc,
      Row(1, "a"), Row(2, "b")
    )

    checkPhysicalPlan(
      r3 orderBy a3.asc.nullsFirst,
      Row(null: Integer, "b"),
      Row(1: Integer, "a"),
      Row(3: Integer, "c"),
      Row(4: Integer, null)
    )

    checkPhysicalPlan(
      r3 orderBy a3.asc.nullsLast,
      Row(1: Integer, "a"),
      Row(3: Integer, "c"),
      Row(4: Integer, null),
      Row(null: Integer, "b")
    )

    checkPhysicalPlan(
      r3 orderBy b3.desc.nullsFirst,
      Row(4: Integer, null),
      Row(3: Integer, "c"),
      Row(null: Integer, "b"),
      Row(1: Integer, "a")
    )

    checkPhysicalPlan(
      r3 orderBy b3.desc.nullsLast,
      Row(3: Integer, "c"),
      Row(null: Integer, "b"),
      Row(1: Integer, "a"),
      Row(4: Integer, null)
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
      r1 cartesian r2 on a1 === a2,
      Row(1, "a", 1, "a")
    )

    checkPhysicalPlan(
      r1 cartesian r2 on a1 > a2,
      Row(2, "b", 1, "a")
    )
  }
}
