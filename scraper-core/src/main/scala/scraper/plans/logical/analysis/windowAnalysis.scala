package scraper.plans.logical.analysis

import scraper._
import scraper.expressions._
import scraper.expressions.windows.{WindowFunction, WindowSpec}
import scraper.plans.logical._
import scraper.plans.logical.analysis.WindowAnalysis._

class ExtractWindowFunctionsFromProjects(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case Resolved(child Project projectList) if hasWindowFunction(projectList) =>
      // Collects all window functions and aliases them.
      val windowAliases = collectWindowFunctions(projectList) map (WindowAlias(_))

      // Replaces all window functions in the original project list with their corresponding
      // `WindowAttribute`s.
      val rewrittenProjectList = {
        val rewrite = windowAliases.map { alias => alias.child -> alias.toAttribute }.toMap
        projectList map (_ transformDown { case e: WindowFunction => rewrite(e) })
      }

      // Builds and stacks `Window` operator(s) over the child plan.
      val windowed = stackWindows(child, windowAliases)

      // Adds an extra projection to ensure that no `GeneratedNamedExpression`s appear in the output
      // attribute list.
      windowed select rewrittenProjectList
  }

  private def hasWindowFunction(expressions: Seq[Expression]): Boolean =
    expressions exists hasWindowFunction

  private def hasWindowFunction(expression: Expression): Boolean =
    expression.collectFirst { case _: WindowFunction => () }.nonEmpty
}

object WindowAnalysis {
  def collectWindowFunctions(expressions: Seq[Expression]): Seq[WindowFunction] =
    expressions.flatMap(_.collect { case f: WindowFunction => f }).distinct

  def stackWindows(plan: LogicalPlan, windowAliases: Seq[WindowAlias]): LogicalPlan = {
    // Finds out all distinct window specs.
    val windowSpecs = windowAliases.map(_.child.window).distinct

    // Groups all window functions by their window specs. We are doing a sort here so that it would
    // be easier to reason about the order of all the generated `Window` operators.
    val windowAliasGroups = windowAliases.groupBy(_.child.window).toSeq sortBy {
      case (window: WindowSpec, _) => windowSpecs indexOf window
    }

    // Builds one `Window` operator builder function for each group.
    val windowBuilders = windowAliasGroups map {
      case (WindowSpec(partitionSpec, orderSpec, _), aliases) =>
        Window(_: LogicalPlan, aliases, partitionSpec, orderSpec)
    }

    // Stacks all windows over the input plan node.
    (windowBuilders foldRight plan) { _ apply _ }
  }
}
