package scraper.plans

import scraper.Context
import scraper.plans.logical.LogicalPlan
import scraper.plans.physical.PhysicalPlan

class QueryExecution(val context: Context, val logicalPlan: LogicalPlan) {
  lazy val analyzedPlan: LogicalPlan = context.queryExecutor analyze logicalPlan

  lazy val optimizedPlan: LogicalPlan = context.queryExecutor optimize analyzedPlan

  lazy val physicalPlan: PhysicalPlan = context.queryExecutor plan optimizedPlan
}
