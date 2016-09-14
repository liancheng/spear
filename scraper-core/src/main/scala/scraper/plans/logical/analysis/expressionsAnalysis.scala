package scraper.plans.logical.analysis

import scala.util.Try

import scraper._
import scraper.exceptions.{AnalysisException, ResolutionFailureException}
import scraper.expressions._
import scraper.expressions.AutoAlias.AnonymousColumnName
import scraper.expressions.aggregates.{AggregateFunction, Count, DistinctAggregateFunction}
import scraper.expressions.functions.lit
import scraper.plans.logical._
import scraper.plans.logical.analysis.ResolveAliases.UnquotedName
import scraper.types.{DataType, StringType}

/**
 * This rule expands "`*`" appearing in `SELECT`.
 */
class ExpandStars(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case Unresolved(Resolved(child) Project projectList) =>
      child select (projectList flatMap {
        case Star(qualifier) => expand(qualifier, child.output)
        case e               => Seq(e)
      })
  }

  private def expand(maybeQualifier: Option[Name], input: Seq[Attribute]): Seq[Attribute] =
    maybeQualifier map { qualifier =>
      input collect {
        case ref: AttributeRef if ref.qualifier contains qualifier => ref
      }
    } getOrElse input
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
      resolveReferences(plan)
  }

  private def resolveReferences(plan: LogicalPlan): LogicalPlan = plan transformExpressionsUp {
    case unresolved @ UnresolvedAttribute(name, qualifier) =>
      def reportResolutionFailure(message: String): Nothing = {
        throw new ResolutionFailureException(
          s"""Failed to resolve attribute ${unresolved.debugString} in logical query plan:
             |${plan.prettyTree}
             |$message
             |""".stripMargin
        )
      }

      val candidates = plan.children flatMap (_.output) collect {
        case a: AttributeRef if a.name == name && qualifier == a.qualifier => a
        case a: AttributeRef if a.name == name && qualifier.isEmpty        => a
      }

      candidates match {
        case Seq(attribute) =>
          attribute

        case Nil =>
          // No candidates found, but we don't report resolution failure here since the attribute
          // might be resolved later with the help of other analysis rules.
          unresolved

        case _ =>
          // Multiple candidates found, something terrible must happened...
          reportResolutionFailure {
            val list = candidates map (_.debugString) mkString ", "
            s"Multiple ambiguous input attributes found: $list"
          }
      }
  }
}

/**
 * This rule converts [[scraper.expressions.AutoAlias AutoAlias]]es into real
 * [[scraper.expressions.Alias Alias]]es, as long as aliased expressions are resolved.
 */
class ResolveAliases(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressionsDown {
    case AutoAlias(Resolved(child: Expression)) =>
      // Uses `UnquotedName` to eliminate back-ticks and double-quotes in generated alias names.
      val alias = child.transformDown {
        case a: AttributeRef                  => UnquotedName(a)
        case Literal(lit: String, StringType) => UnquotedName(lit)
      }.sql getOrElse AnonymousColumnName

      child as Name.caseSensitive(alias)
  }
}

object ResolveAliases {
  /**
   * Auxiliary class only used for removing back-ticks and double-quotes from auto-generated column
   * names. For example, for the following SQL query:
   * {{{
   *   SELECT concat("hello", "world")
   * }}}
   * should produce a column named as
   * {{{
   *   concat(hello, world)
   * }}}
   * instead of
   * {{{
   *   concat('hello', 'world')
   * }}}
   */
  private case class UnquotedName(named: NamedExpression)
    extends LeafExpression with UnevaluableExpression {

    override lazy val isResolved: Boolean = named.isResolved

    override lazy val dataType: DataType = named.dataType

    override lazy val isNullable: Boolean = named.isNullable

    override def sql: Try[String] = Try(named.name.casePreserving)
  }

  private object UnquotedName {
    def apply(stringLiteral: String): UnquotedName =
      UnquotedName(lit(stringLiteral) as Name.caseSensitive(stringLiteral))
  }
}

/**
 * This rule resolves [[scraper.expressions.UnresolvedFunction unresolved functions]] by looking
 * up function names from the [[Catalog]].
 */
class ResolveFunctions(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressionsDown {
    case UnresolvedFunction(name, Seq(_: Star), isDistinct @ false) if name == i"count" =>
      Count(1)

    case Count((_: Star)) =>
      Count(1)

    case UnresolvedFunction(_, Seq(_: Star), isDistinct @ true) =>
      throw new AnalysisException("DISTINCT cannot be used together with star")

    case UnresolvedFunction(name, Seq(_: Star), _) =>
      throw new AnalysisException("Only function \"count\" may have star as argument")

    case UnresolvedFunction(name, args, isDistinct) if args forall (_.isResolved) =>
      val fnInfo = catalog.functionRegistry.lookupFunction(name)
      fnInfo.builder(args) match {
        case f: AggregateFunction if isDistinct =>
          DistinctAggregateFunction(f)

        case f if isDistinct =>
          throw new AnalysisException(
            s"Cannot decorate function $name with DISTINCT since it is not an aggregate function"
          )

        case f =>
          f
      }
  }
}

