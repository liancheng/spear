package scraper.plans.logical.analysis

import scraper._
import scraper.expressions._
import scraper.expressions.windows.{WindowFunction, WindowSpec}
import scraper.plans.logical._

class ExtractWindowFunctions(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case child Project projectList if containsWindowFunction(projectList) =>
      val windowAliases = collectWindowFunctions(projectList).distinct map (WindowAlias(_))

      val groupedByWindow = {
        val windowSpecs = windowAliases.map(_.child.window).distinct
        windowAliases.groupBy(_.child.window).toSeq.sortBy {
          case (window: WindowSpec, _) => windowSpecs indexOf window
        }
      }

      val windowBuilders = groupedByWindow map {
        case (WindowSpec(partitionSpec, orderSpec, _), aliases) =>
          Window(_: LogicalPlan, aliases, partitionSpec, orderSpec)
      }

      (windowBuilders foldRight child) { _ apply _ }
  }

  private def collectWindowFunctions(projectList: Seq[NamedExpression]): Seq[WindowFunction] =
    projectList flatMap (_.collect { case f: WindowFunction => f })

  private def containsWindowFunction(projectList: Seq[NamedExpression]): Boolean =
    projectList exists (_.collectFirst { case _: WindowFunction => () }.nonEmpty)
}
