package scraper.plans

import scraper.Context
import scraper.plans.logical.LogicalPlan
import scraper.plans.physical.PhysicalPlan
import scraper.trees.RulesExecutor

trait QueryExecution {
  def context: Context

  def logicalPlan: LogicalPlan

  lazy val analyzedPlan: LogicalPlan = context.analyzer(logicalPlan)

  lazy val optimizedPlan: LogicalPlan = context.optimizer(analyzedPlan)

  lazy val physicalPlan: PhysicalPlan = context.planner.plan(optimizedPlan)
}
