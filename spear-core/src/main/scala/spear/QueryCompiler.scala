package spear

import spear.parsers.DirectlyExecutableStatementParser.directlyExecutableStatement
import spear.plans.CompiledQuery
import spear.plans.logical.{LogicalPlan, Optimizer}
import spear.plans.logical.analysis.Analyzer
import spear.plans.physical.PhysicalPlan

trait QueryCompiler {
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

  def compile(context: Context, plan: LogicalPlan): CompiledQuery = new CompiledQuery(context, plan)
}

trait BasicQueryCompiler extends QueryCompiler {
  override val catalog: Catalog = new InMemoryCatalog

  override def parse(query: String): LogicalPlan = {
    import fastparse.all._

    (Start ~ directlyExecutableStatement ~ End parse query).get.value
  }

  override def analyze(plan: LogicalPlan): LogicalPlan = analyzer apply plan

  override def optimize(plan: LogicalPlan): LogicalPlan = optimizer apply plan

  private val analyzer = new Analyzer(catalog)

  private val optimizer = new Optimizer
}
