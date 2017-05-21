package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.WindowAnalysisException
import scraper.expressions._
import scraper.expressions.InternalAlias.buildRewriter
import scraper.expressions.windows.{WindowFunction, WindowSpecRef}
import scraper.plans.logical._
import scraper.plans.logical.analysis.AggregationAnalysis.hasAggregateFunction
import scraper.plans.logical.analysis.WindowAnalysis._
import scraper.plans.logical.patterns.Resolved
import scraper.utils._

/**
 * This rule extracts window functions from `SELECT` and `ORDER BY` clauses and moves them into
 * separate `Window` operators.
 */
class ExtractWindowFunctions(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan =
    tree collectFirst skip map { _ => tree } getOrElse rewrite(tree)

  private def rewrite(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case Resolved(child Sort order) if hasWindowFunction(order) =>
      val winAliases = collectWindowFunctions(order) map { WindowAlias(_) }
      val rewrittenOrder = order map { _ transformDown buildRewriter(winAliases) }
      child windows winAliases sort rewrittenOrder select child.output

    case Resolved(child Project projectList) if hasWindowFunction(projectList) =>
      val winAliases = collectWindowFunctions(projectList) map { WindowAlias(_) }
      val rewrittenProjectList = projectList map { _ transformDown buildRewriter(winAliases) }
      child windows winAliases select rewrittenProjectList
  }

  private val skip: PartialFunction[LogicalPlan, Unit] = {
    // Waits until all aggregations are resolved.
    case _: UnresolvedAggregate =>

    // Waits until all SQL `ORDER BY` clauses are resolved.
    case _: UnresolvedSort =>

    // Waits until all global aggregations are resolved.
    case Resolved(_ Project projectList) if hasAggregateFunction(projectList) =>
  }
}

class InlineWindowDefinitions(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case WindowDef(child, name, windowSpec) =>
      child transformAllExpressionsDown {
        case WindowSpecRef(`name`, maybeFrame) =>
          for {
            existingFrame <- windowSpec.windowFrame
            newFrame <- maybeFrame
          } throw new WindowAnalysisException(
            s"""Cannot decorate window $name with frame $newFrame
               |because it already has frame $existingFrame
               |""".oneLine
          )

          windowSpec between (windowSpec.windowFrame orElse maybeFrame)
      }
  }
}

object WindowAnalysis {
  def hasWindowFunction(expressions: Seq[Expression]): Boolean =
    expressions exists hasWindowFunction

  /**
   * Collects all distinct window functions from `expressions`.
   */
  def collectWindowFunctions(expressions: Seq[Expression]): Seq[WindowFunction] =
    (expressions flatMap collectWindowFunctions).distinct

  /**
   * Collects all distinct window functions from `expression`.
   */
  def collectWindowFunctions(expression: Expression): Seq[WindowFunction] =
    expression.collect { case e: WindowFunction => e }.distinct

  private def hasWindowFunction(expression: Expression): Boolean =
    expression.collectFirst { case _: WindowFunction => }.nonEmpty
}
