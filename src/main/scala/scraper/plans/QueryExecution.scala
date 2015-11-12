package scraper.plans

import scraper.Context
import scraper.plans.logical.LogicalPlan
import scraper.plans.physical.PhysicalPlan
import scraper.trees.RulesExecutor

trait QueryExecution {
  protected def analyzer: RulesExecutor[LogicalPlan]

  protected def optimizer: RulesExecutor[LogicalPlan]

  protected def planner: QueryPlanner[LogicalPlan, PhysicalPlan]

  def context: Context

  def logicalPlan: LogicalPlan

  lazy val analyzedPlan: LogicalPlan = analyzer(logicalPlan)

  lazy val optimizedPlan: LogicalPlan = optimizer(analyzedPlan)

  lazy val physicalPlan: PhysicalPlan = planner.plan(optimizedPlan)
}
