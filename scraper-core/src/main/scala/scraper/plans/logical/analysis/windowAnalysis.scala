package scraper.plans.logical.analysis

import scraper._
import scraper.expressions._
import scraper.expressions.windows.{WindowFunction, WindowSpec}
import scraper.plans.logical._
import scraper.plans.logical.analysis.WindowAnalysis.hasWindowFunction

class ExtractWindowFunctions(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case Resolved(child Project projectList) if hasWindowFunction(projectList) =>
      extractWindowFunctions(child, projectList)

    case agg: UnresolvedAggregate if agg.projectList exists (!_.isResolved) =>
      agg

    case agg: UnresolvedAggregate if hasWindowFunction(agg.projectList) =>
      throw new UnsupportedOperationException(
        s"Window functions in aggregation is not supported yet"
      )
  }

  private def extractWindowFunctions(
    child: LogicalPlan, projectList: Seq[NamedExpression]
  ): LogicalPlan = {
    // Collects all window functions and aliases them.
    val windowAliases = collectWindowFunctions(projectList) map (WindowAlias(_))

    val windowBuilders = {
      // Finds out all distinct window specs.
      val windowSpecs = windowAliases.map(_.child.window).distinct

      // Groups all window functions by their window specs and then build one `Window` operator
      // builder function for each group. We are doing a sort here so that it would be easier to
      // reason about the order of all the generated `Window` operators.
      windowAliases.groupBy(_.child.window).toSeq sortBy {
        case (window: WindowSpec, _) => windowSpecs indexOf window
      } map {
        case (spec, aliases) =>
          (_: LogicalPlan) window aliases partitionBy spec.partitionSpec orderBy spec.orderSpec
      }
    }

    // Builds and stacks all the `Window` operators.
    val windowed = (windowBuilders foldRight child) { _ apply _ }

    // Replaces all window functions in the original project list with their corresponding
    // `WindowAttribute`s.
    val rewrittenProjectList = {
      val rewrite = windowAliases.map { alias => alias.child -> alias.toAttribute }.toMap
      projectList.map(_ transformDown { case e: WindowFunction => rewrite(e) })
    }

    // Stacks a top-level projection so that no `GeneratedNamedExpression`s appear in the final
    // output attribute list.
    windowed select rewrittenProjectList
  }

  private def collectWindowFunctions(projectList: Seq[NamedExpression]): Seq[WindowFunction] =
    projectList.flatMap(_.collect { case f: WindowFunction => f }).distinct
}

object WindowAnalysis {
  private[analysis] def hasWindowFunction(projectList: Seq[NamedExpression]): Boolean =
    projectList exists (_.collectFirst { case _: WindowFunction => () }.nonEmpty)
}
