package scraper.plans.logical.analysis

import scraper._
import scraper.expressions._
import scraper.expressions.InternalAlias.buildRewriter
import scraper.expressions.windows.{WindowFunction, WindowSpec}
import scraper.plans.logical._
import scraper.plans.logical.analysis.WindowAnalysis._

class ExtractWindowFunctionsFromProjects(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case Resolved(child Project projectList) if hasWindowFunction(projectList) =>
      val winAliases = collectWindowFunctions(projectList) map (WindowAlias(_))
      val rewrittenProjectList = projectList map (_ transformDown buildRewriter(winAliases))
      child windows winAliases select rewrittenProjectList
  }
}

class ExtractWindowFunctionsFromSorts(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan =
    tree collectFirst preConditionViolation map (_ => tree) getOrElse {
      tree transformDown {
        case Resolved(child Sort order) if hasWindowFunction(order) =>
          val winAliases = collectWindowFunctions(order) map (WindowAlias(_))
          val rewrittenOrder = order map (_ transformDown buildRewriter(winAliases))
          child windows winAliases orderBy rewrittenOrder select child.output
      }
    }

  private val preConditionViolation: PartialFunction[LogicalPlan, Unit] = {
    case _: UnresolvedAggregate =>
  }
}

object WindowAnalysis {
  def hasWindowFunction(expressions: Seq[Expression]): Boolean =
    expressions exists hasWindowFunction

  def hasWindowFunction(expression: Expression): Boolean =
    expression.collectFirst { case _: WindowFunction => }.nonEmpty

  /**
   * Collects all distinct window functions from `expressions`.
   */
  def collectWindowFunctions(expressions: Seq[Expression]): Seq[WindowFunction] =
    expressions.flatMap(_.collect { case f: WindowFunction => f }).distinct

  /**
   * Given a logical `plan` and a list of one or more window functions, stacks one or more
   * [[Window]] operators over `plan`.
   */
  def stackWindows(plan: LogicalPlan, windowAliases: Seq[WindowAlias]): Window = {
    assert(windowAliases.nonEmpty)
    windowBuilders(windowAliases) reduce { _ andThen _ } apply plan
  }

  /**
   * Given a logical `plan` and a list of zero or more window functions, stacks zero or more
   * [[Window]] operators over `plan`.
   */
  def stackWindowsOption(plan: LogicalPlan, windowAliases: Seq[WindowAlias]): LogicalPlan =
    (windowBuilders(windowAliases) foldLeft plan) { (p, f) => f apply p }

  private def windowBuilders(windowAliases: Seq[WindowAlias]): Seq[LogicalPlan => Window] = {
    // Finds out all distinct window specs.
    val windowSpecs = windowAliases.map(_.child.window).distinct

    // Groups all window functions by their window specs. We are doing sorts here so that it would
    // be easier to reason about the order of all the generated `Window` operators.
    val windowAliasGroups = windowAliases
      .groupBy(_.child.window)
      .mapValues(_ sortBy windowAliases.indexOf)
      .toSeq
      .sortBy { case (spec: WindowSpec, _) => windowSpecs indexOf spec }

    // Builds one `Window` operator builder function for each group.
    windowAliasGroups map {
      case (WindowSpec(partitionSpec, orderSpec, _), aliases) =>
        Window(_: LogicalPlan, aliases, partitionSpec, orderSpec)
    }
  }
}
