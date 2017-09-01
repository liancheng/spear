package spear.plans.logical.analysis

import spear._
import spear.exceptions.WindowAnalysisException
import spear.expressions._
import spear.expressions.InternalAlias.buildRewriter
import spear.expressions.windows.{WindowFunction, WindowSpecRef}
import spear.plans.logical._
import spear.plans.logical.analysis.AggregationAnalysis.hasAggregateFunction
import spear.plans.logical.analysis.WindowAnalysis._
import spear.plans.logical.patterns.Resolved
import spear.utils._

/**
 * This rule extracts window functions from `SELECT` and `ORDER BY` clauses and moves them into
 * separate `Window` operators.
 */
class ExtractWindowFunctions(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan =
    tree collectFirstDown skip map { _ => tree } getOrElse rewrite(tree)

  private def rewrite(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case Resolved(child Sort order) if hasWindowFunction(order) =>
      val winAliases = collectWindowFunctions(order) map { WindowFunctionAlias(_) }
      val rewrittenOrder = order map { _ transformDown buildRewriter(winAliases) }
      child windows winAliases sort rewrittenOrder select child.output

    case Resolved(child Project projectList) if hasWindowFunction(projectList) =>
      val winAliases = collectWindowFunctions(projectList) map { WindowFunctionAlias(_) }
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
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case WindowDef(child, name, windowSpec) =>
      child transformDown {
        case node =>
          node transformExpressionsDown {
            case WindowSpecRef(`name`, maybeFrame) =>
              for {
                existingFrame <- windowSpec.windowFrame
                newFrame <- maybeFrame
              } throw new WindowAnalysisException(
                s"""Cannot decorate window $name with frame $newFrame
                   |because it already has a frame $existingFrame
                   |""".oneLine
              )

              windowSpec between (windowSpec.windowFrame orElse maybeFrame)
          }
      }
  }
}

class RejectUndefinedWindowSpecRef(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = {
    tree collectDown {
      case node =>
        val names = node.expressions flatMap {
          _ collectDown {
            case WindowSpecRef(name, _) => name
          }
        }

        if (names.nonEmpty) {
          throw new WindowAnalysisException(
            s"Undefined window specification references detected: ${names mkString ", "}"
          )
        }
    }

    tree
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
    expression.collectDown { case e: WindowFunction => e }.distinct

  private def hasWindowFunction(expression: Expression): Boolean =
    expression.collectFirstDown { case _: WindowFunction => }.nonEmpty
}
