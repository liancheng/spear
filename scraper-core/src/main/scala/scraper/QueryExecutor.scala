package scraper

import scraper.plans.QueryExecution
import scraper.plans.logical.LogicalPlan
import scraper.plans.physical.PhysicalPlan

trait QueryExecutor {
  def catalog: Catalog

  /**
   * Parses given query string to a [[scraper.plans.logical.LogicalPlan logical plan]].
   */
  def parse(query: String): LogicalPlan

  /**
   * Analyzes an unresolved [[scraper.plans.logical.LogicalPlan logical plan]] and outputs its
   * strictly-typed version.
   */
  def analyze(plan: LogicalPlan): LogicalPlan

  /**
   * Optimizes a resolved [[scraper.plans.logical.LogicalPlan logical plan]] into another equivalent
   * but more performant version.
   */
  def optimize(plan: LogicalPlan): LogicalPlan

  /**
   * Plans a [[scraper.plans.logical.LogicalPlan logical plan]] into an executable
   * [[scraper.plans.physical.PhysicalPlan physical plan]].
   */
  def plan(plan: LogicalPlan): PhysicalPlan

  def execute(context: Context, plan: LogicalPlan): QueryExecution
}
