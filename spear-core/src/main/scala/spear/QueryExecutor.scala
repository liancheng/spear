package spear

import spear.plans.QueryExecution
import spear.plans.logical.LogicalPlan
import spear.plans.physical.PhysicalPlan

trait QueryExecutor {
  def catalog: Catalog

  /**
   * Parses given query string to a [[spear.plans.logical.LogicalPlan logical plan]].
   */
  def parse(query: String): LogicalPlan

  /**
   * Analyzes an unresolved [[spear.plans.logical.LogicalPlan logical plan]] and outputs its
   * strictly-typed version.
   */
  def analyze(plan: LogicalPlan): LogicalPlan

  /**
   * Optimizes a resolved [[spear.plans.logical.LogicalPlan logical plan]] into another equivalent
   * but more performant version.
   */
  def optimize(plan: LogicalPlan): LogicalPlan

  /**
   * Plans a [[spear.plans.logical.LogicalPlan logical plan]] into an executable
   * [[spear.plans.physical.PhysicalPlan physical plan]].
   */
  def plan(plan: LogicalPlan): PhysicalPlan

  def execute(context: Context, plan: LogicalPlan): QueryExecution =
    new QueryExecution(context, plan)
}
