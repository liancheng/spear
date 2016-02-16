package scraper

import scraper.plans.logical.LogicalPlan

class SQLBuilder(logicalPlan: LogicalPlan, context: Context) {
  def build(): Option[String] = None
}
