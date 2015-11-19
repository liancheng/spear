package scraper.plans

import scraper.utils.Logging

trait QueryPlanner[Logical <: QueryPlan[Logical], Physical <: QueryPlan[Physical]] extends Logging {
  trait Strategy {
    def apply(logicalPlan: Logical): Seq[Physical]
  }

  def strategies: Seq[Strategy]

  def planLater(logicalPlan: Logical): Physical = plan(logicalPlan)

  private def plan(logicalPlan: Logical): Physical = {
    val physicalPlans = for {
      strategy <- strategies
      physicalPlan <- strategy(logicalPlan)
    } yield physicalPlan

    assert(
      physicalPlans.nonEmpty,
      s"""Failed to compile logical query plan
         |
         |${logicalPlan.prettyTree}
       """.stripMargin
    )

    physicalPlans.head
  }

  def apply(logicalPlan: Logical): Physical = {
    logTrace(
      s"""Planning logical query plan:
         |
         |${logicalPlan.prettyTree}
       """.stripMargin
    )

    val physicalPlan = plan(logicalPlan)

    logTrace(
      s"""Compiled logical query plan
         |
         |${logicalPlan.prettyTree}
         |
         |to physical query plan
         |
         |${physicalPlan.prettyTree}
       """.stripMargin
    )

    physicalPlan
  }
}
