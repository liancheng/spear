package scraper.plans.logical.analysis

import scala.util.control.NonFatal

import scraper._
import scraper.exceptions.{AnalysisException, ResolutionFailureException}
import scraper.expressions._
import scraper.expressions.NamedExpression.AnonymousColumnName
import scraper.expressions.aggregates.{AggregateFunction, Count, DistinctAggregateFunction}
import scraper.plans.logical._

/**
 * This rule expands "`*`" appearing in `SELECT`.
 */
class ExpandStars(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case Unresolved(Project(projectList, Resolved(child))) =>
      child select (projectList flatMap {
        case Star(qualifier) => expand(qualifier, child.output)
        case e               => Seq(e)
      })
  }

  private def expand(maybeQualifier: Option[Name], input: Seq[Attribute]): Seq[Attribute] =
    (maybeQualifier fold input) { qualifier =>
      input collect {
        case ref: AttributeRef if ref.qualifier contains qualifier => ref
      }
    }
}

/**
 * This rule tries to resolve [[scraper.expressions.UnresolvedAttribute UnresolvedAttribute]]s in
 * an logical plan operator using output [[scraper.expressions.Attribute Attribute]]s of its
 * children.
 *
 * @throws scraper.exceptions.ResolutionFailureException If no candidate or multiple ambiguous
 *         candidate input attributes can be found.
 */
class ResolveReferences(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case Unresolved(plan) if plan.isDeduplicated =>
      val input = plan.children flatMap { _.output }
      plan transformExpressionsDown {
        case a: UnresolvedAttribute =>
          try a tryResolveUsing input catch {
            case NonFatal(cause) =>
              throw new ResolutionFailureException(
                s"""Failed to resolve attribute ${a.sqlLike} in logical plan:
                   |
                   |${plan.prettyTree}
                   |""".stripMargin,
                cause
              )
          }
      }
  }
}

/**
 * This rule converts [[scraper.expressions.UnresolvedAlias unresolved aliases]] into proper
 * [[scraper.expressions.Alias aliases]].
 */
class ResolveAliases(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressionsDown {
    case UnresolvedAlias(Resolved(child: Expression)) =>
      child as Name.caseSensitive(child.sql getOrElse AnonymousColumnName)
  }
}

/**
 * This rule resolves [[scraper.expressions.UnresolvedFunction unresolved functions]] by looking
 * up function names from the [[Catalog]].
 */
class ResolveFunctions(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressionsDown {
    case UnresolvedFunction(name, Seq(_: Star), false) if name == i"count" =>
      Count(1)

    case Count(_: Star) =>
      Count(1)

    case UnresolvedFunction(_, Seq(_: Star), true) =>
      throw new AnalysisException("DISTINCT cannot be used together with star")

    case UnresolvedFunction(_, Seq(_: Star), _) =>
      throw new AnalysisException("Only function \"count\" may have star as argument")

    case UnresolvedFunction(name, args, isDistinct) if args forall { _.isResolved } =>
      val fnInfo = catalog.functionRegistry lookupFunction name
      fnInfo.builder apply args match {
        case f: AggregateFunction if isDistinct =>
          DistinctAggregateFunction(f)

        case _ if isDistinct =>
          throw new AnalysisException(
            s"Cannot decorate function $name with DISTINCT since it is not an aggregate function"
          )

        case f =>
          f
      }
  }
}
