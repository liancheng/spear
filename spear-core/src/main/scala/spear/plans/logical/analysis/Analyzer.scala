package spear.plans.logical.analysis

import spear._
import spear.exceptions.AnalysisException
import spear.expressions._
import spear.expressions.NamedExpression.newExpressionID
import spear.plans.logical._
import spear.plans.logical.analysis.AggregationAnalysis.hasAggregateFunction
import spear.plans.logical.patterns.Resolved
import spear.trees._

class Analyzer(catalog: Catalog) extends Transformer(Analyzer.defaultRules(catalog)) {
  override def apply(tree: LogicalPlan): LogicalPlan = {
    logDebug(
      s"""Analyzing logical query plan:
         |
         |${tree.prettyTree}
         |""".stripMargin
    )

    super.apply(tree)
  }
}

object Analyzer {
  def defaultRules(catalog: Catalog): Seq[RuleGroup[LogicalPlan]] = Seq(
    // Desugaring
    RuleGroup(FixedPoint, Seq(
      new InlineCTERelations(catalog),
      new InlineWindowDefinitions(catalog),
      new RejectUndefinedWindowSpecRefs(catalog)
    )),

    RuleGroup(FixedPoint, Seq(
      RuleGroup(FixedPoint, Seq(
        RuleGroup(FixedPoint, Seq(
          new ResolveRelations(catalog),
          new RenameCTEColumns(catalog),
          new DeduplicateReferences(catalog),

          // Expression resolution
          new ExpandStars(catalog),
          new ResolveReferences(catalog),
          new ResolveFunctions(catalog),
          new ResolveAliases(catalog)
        )),

        new ResolveOrderByClauses(catalog),
        new DiscoverGlobalAggregations(catalog)
      )),

      // Rules that help resolving window functions
      new ExtractWindowFunctionsFromSorts(catalog),
      new ExtractWindowFunctionsFromProjections(catalog),

      // Rules that help resolving aggregations
      new RewriteDistinctAggregateFunctions(catalog),
      new RewriteDistinctProjections(catalog),
      new UnifyFilteredSortedAggregate(catalog),
      new ExpandUnresolvedAggregates(catalog)
    )),

    RuleGroup(Once, Seq(
      // Type checking
      new EnforceTypeConstraints(catalog),

      // Post-analysis checks
      new RejectUnresolvedExpressions(catalog),
      new RejectUnresolvedPlans(catalog),
      new RejectTopLevelInternalAttributes(catalog),
      new RejectDistinctAggregateFunctions(catalog),
      new RejectOrphanAttributeRefs(catalog)
    ))
  )
}

trait AnalysisRule extends Rule[LogicalPlan] {
  val catalog: Catalog
}

/**
 * This rule inlines CTE relations as sub-queries. E.g., effectively, it transforms
 * {{{
 *   WITH
 *     c0 AS (SELECT * FROM t0),
 *     c1 AS (SELECT * FROM t1)
 *   SELECT * FROM c0
 *   UNION ALL
 *   SELECT * FROM c1
 * }}}
 * into
 * {{{
 *   SELECT * FROM (SELECT * FROM t0) c0
 *   UNION ALL
 *   SELECT * FROM (SELECT * FROM t1) c1
 * }}}
 */
class InlineCTERelations(val catalog: Catalog) extends AnalysisRule {
  // Uses `transformUp` to inline all CTE relations from bottom up since inner CTE relations may
  // shadow outer CTE relations with the same names.
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case With(child, name, query, maybeAliases) =>
      child transformUp {
        case UnresolvedRelation(`name`) =>
          (maybeAliases fold query) { query rename _ } subquery name
      }
  }
}

/**
 * This rule resolves unresolved relations by looking up the table name from the `catalog`.
 */
class ResolveRelations(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case UnresolvedRelation(name) => catalog lookupRelation name
  }
}

/**
 * This rule rewrites [[Rename]] operators into [[Project projections]] to help resolving CTE
 * queries.
 */
class RenameCTEColumns(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case Resolved(child) Rename aliases Subquery name if child.output.length >= aliases.length =>
      val aliased = (child.output, aliases).zipped map { _ as _ }
      child select (aliased ++ (child.output drop aliases.length)) subquery name

    case Resolved(child) Rename aliases Subquery name =>
      val expected = child.output.length
      val actual = aliases.length
      throw new AnalysisException(
        s"WITH query $name has $expected columns available but $actual columns specified"
      )
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
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case plan if plan.children.length < 2 =>
      plan

    case plan if plan.children exists { !_.isResolved } =>
      plan

    case plan: BinaryLogicalPlan if !plan.isDeduplicated =>
      plan.withChildren(plan.left :: deduplicateRight(plan.left, plan.right) :: Nil)
  }

  private def deduplicateRight(left: LogicalPlan, right: LogicalPlan): LogicalPlan = {
    val conflictingAttributes = left.outputSet intersectByID right.outputSet

    def hasDuplicates(attributes: Set[Attribute]): Boolean =
      (attributes intersectByID conflictingAttributes).nonEmpty

    def rewriteExpressionIDs(oldPlan: LogicalPlan, newPlan: LogicalPlan): LogicalPlan = {
      val rewrite = {
        val oldIDs = oldPlan.output.map { _.expressionID }
        val newIDs = newPlan.output.map { _.expressionID }
        (oldIDs zip newIDs).toMap
      }

      right transformDown {
        case plan if plan == oldPlan => newPlan
      } transformDown {
        case node =>
          node transformExpressionsDown {
            case a: AttributeRef => a withID rewrite.getOrElse(a.expressionID, a.expressionID)
          }
      }
    }

    val maybeDuplicated = right collectFirstDown {
      // Handles relations that introduce ambiguous attributes
      case plan: MultiInstanceRelation if hasDuplicates(plan.outputSet) =>
        plan -> plan.newInstance()

      // Handles projections that introduce ambiguous aliases
      case plan @ Project(_, projectList) if hasDuplicates(collectAliases(projectList)) =>
        val newProjectList = projectList map {
          case a: Alias => a withID newExpressionID()
          case e        => e
        }

        plan -> plan.copy(projectList = newProjectList)
    }

    maybeDuplicated map {
      case (oldPlan, newPlan) =>
        rewriteExpressionIDs(oldPlan, newPlan)
    } getOrElse right
  }

  private def collectAliases(projectList: Seq[NamedExpression]): Set[Attribute] =
    projectList.collect { case a: Alias => a.attr }.toSet
}

class ResolveOrderByClauses(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case child Project projectList Sort order if shouldRewrite(order) =>
      val unevaluableOrder = order.filter { e =>
        !e.isResolved || hasAggregateFunction(e)
      }

      val sortOrderAliases = unevaluableOrder
        .map { _.child }
        .map { _ unaliasUsing projectList }
        .zipWithIndex
        .map { case (e, index) => SortOrderAlias(e, s"order$index") }

      val rewrite = sortOrderAliases.map { e =>
        // Here we map `e.child` to `e.name` instead of `e.attr` because `e` may not be resolved yet
        // and cannot derive a proper `AttributeRef` due to the missing data type information.
        e.child -> (e.name: Expression)
      }.toMap

      val rewrittenOrder = order map { _ transformDown rewrite }
      val output = projectList map { _.attr }

      child select projectList ++ sortOrderAliases orderBy rewrittenOrder select output
  }

  private def shouldRewrite(order: Seq[SortOrder]): Boolean =
    order.exists { !_.isResolved } || hasAggregateFunction(order)
}

/**
 * This rule tries to transform all resolved logical plans operators (and expressions within them)
 * into strictly-typed form.
 *
 * @throws spear.exceptions.AnalysisException If some resolved logical query plan operator
 *         doesn't type check.
 */
class EnforceTypeConstraints(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case Resolved(plan) => plan.strictlyTyped
  }
}
