package scraper

import scraper.plans.execution.PhysicalPlan
import scraper.plans.logical.LogicalPlan

trait QueryExecution {
  def context: Context

  def logicalPlan: LogicalPlan

  def analyzedPlan: LogicalPlan

  def optimizedPlan: LogicalPlan

  def physicalPlan: PhysicalPlan
}
