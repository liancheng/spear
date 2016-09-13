package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.{AnalysisException, ResolutionFailureException}
import scraper.expressions._
import scraper.expressions.AutoAlias.AnonymousColumnName
import scraper.expressions.NamedExpression.{newExpressionID, UnquotedName}
import scraper.expressions.aggregates.{AggregateFunction, Count, DistinctAggregateFunction}
import scraper.plans.logical._
import scraper.types.StringType

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
 * This rule resolves ambiguous duplicated attributes/aliases introduced by binary logical query
 * plan operators like [[Join]] and [[SetOperator set operators]]. For example:
 * {{{
 *   // Self-join, equivalent to "SELECT * FROM t INNER JOIN t":
 *   val df = context table "t"
 *   val joined = df join df
 *
 *   // Self-union, equivalent to "SELECT 1 AS a UNION ALL SELECT 1 AS a":
 *   val df = context single (1 as 'a)
 *   val union = df union df
 * }}}
 */
class DeduplicateReferences(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case plan if plan.children.forall(_.isResolved) && !plan.isDeduplicated =>
      plan match {
        case node: Join      => node.copy(right = deduplicateRight(node.left, node.right))
        case node: Union     => node.copy(right = deduplicateRight(node.left, node.right))
        case node: Intersect => node.copy(right = deduplicateRight(node.left, node.right))
        case node: Except    => node.copy(right = deduplicateRight(node.left, node.right))
      }
  }

  def deduplicateRight(left: LogicalPlan, right: LogicalPlan): LogicalPlan = {
    val conflictingAttributes = left.outputSet intersectByID right.outputSet

    def hasDuplicates(attributes: Set[Attribute]): Boolean =
      (attributes intersectByID conflictingAttributes).nonEmpty

    right collectFirst {
      // Handles relations that introduce ambiguous attributes
      case plan: MultiInstanceRelation if hasDuplicates(plan.outputSet) =>
        plan -> plan.newInstance()

      // Handles projections that introduce ambiguous aliases
      case plan @ Project(_, projectList) if hasDuplicates(collectAliases(projectList)) =>
        plan -> plan.copy(projectList = projectList map {
          case a: Alias => a withID newExpressionID()
          case e        => e
        })
    } map {
      case (oldPlan, newPlan) =>
        val rewrite = {
          val oldIDs = oldPlan.output map (_.expressionID)
          val newIDs = newPlan.output map (_.expressionID)
          (oldIDs zip newIDs).toMap
        }

        right transformDown {
          case plan if plan == oldPlan => newPlan
        } transformAllExpressionsDown {
          case a: AttributeRef => rewrite get a.expressionID map a.withID getOrElse a
        }
    } getOrElse right
  }

  private def collectAliases(projectList: Seq[NamedExpression]): Set[Attribute] =
    projectList.collect { case a: Alias => a.toAttribute }.toSet
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

      child as Name.caseInsensitive(alias)
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

