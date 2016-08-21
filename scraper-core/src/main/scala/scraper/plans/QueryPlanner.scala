package scraper.plans

import scraper.trees.TreeNode
import scraper.utils.Logging

trait QueryPlanner[Logical <: QueryPlan[Logical], Physical <: TreeNode[Physical]] extends Logging {
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
      s"""Don't know how to compile the following logical query plan fragment:
         |
         |${logicalPlan.prettyTree}
         |""".stripMargin
    )

    physicalPlans.head
  }

  def apply(logicalPlan: Logical): Physical = {
    logDebug(
      s"""Planning logical query plan:
         |
         |${logicalPlan.prettyTree}
         |""".stripMargin
    )

    val physicalPlan = plan(logicalPlan)

    logDebug(
      s"""Compiled logical query plan
         |
         |${logicalPlan.prettyTree}
         |
         |to physical query plan
         |
         |${physicalPlan.prettyTree}
         |""".stripMargin
    )

    physicalPlan
  }
}
