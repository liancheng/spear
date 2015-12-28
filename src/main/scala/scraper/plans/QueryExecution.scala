package scraper.plans

import scraper.Context
import scraper.plans.logical.LogicalPlan
import scraper.plans.physical.PhysicalPlan

trait QueryExecution {
  def context: Context

  def logicalPlan: LogicalPlan

  lazy val analyzedPlan: LogicalPlan = context analyze logicalPlan

  lazy val optimizedPlan: LogicalPlan = context optimize analyzedPlan

  lazy val physicalPlan: PhysicalPlan = context plan optimizedPlan

  lazy val sql: Option[String] = analyzedPlan.sql(context)
}
