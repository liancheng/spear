package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.AnalysisException
import scraper.expressions._
import scraper.expressions.NamedExpression.newExpressionID
import scraper.plans.logical._
import scraper.plans.logical.analysis.AggregationAnalysis.haveAggregateFunction
import scraper.plans.logical.patterns.{Resolved, Unresolved}
import scraper.trees.{Rule, RulesExecutor}
import scraper.trees.RulesExecutor.{FixedPoint, Once}

class Analyzer(catalog: Catalog) extends RulesExecutor[LogicalPlan] {
  override def batches: Seq[RuleBatch] = Seq(
    RuleBatch("Pre-processing", FixedPoint.Unlimited, Seq(
      new RewriteCTEsAsSubquery(catalog),
      new InlineWindowDefinitions(catalog)
    )),

    RuleBatch("Pre-processing check", Once, Seq( // TODO Check for undefined window references
    )),

    RuleBatch("Resolution", FixedPoint.Unlimited, Seq(
      new ResolveRelations(catalog),
      new RewriteRenamesToProjects(catalog),
      new ResolveSortReferences(catalog),
      new DeduplicateReferences(catalog),

      // Rules that help resolving expressions
      new ExpandStars(catalog),
      new ResolveReferences(catalog),
      new ResolveFunctions(catalog),
      new ResolveAliases(catalog),

      // Rules that help resolving window functions
      new ExtractWindowFunctionsFromProjects(catalog),
      new ExtractWindowFunctionsFromSorts(catalog),

      // Rules that help resolving aggregations
      new RewriteDistinctAggregateFunctions(catalog),
      new RewriteDistinctsAsAggregates(catalog),
      new RewriteProjectsAsGlobalAggregates(catalog),
      new UnifyFilteredSortedAggregates(catalog),
      new RewriteUnresolvedAggregates(catalog)
    )),

    RuleBatch("Type check", Once, Seq(
      new EnforceTypeConstraints(catalog)
    )),

    RuleBatch("Post-analysis check", Once, Seq(
      new RejectUnresolvedExpressions(catalog),
      new RejectUnresolvedPlans(catalog),
      new RejectTopLevelInternalAttributes(catalog),
      new RejectDistinctAggregateFunctions(catalog),
      new RejectOrphanAttributeReferences(catalog)
    ))
  )

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

trait AnalysisRule extends Rule[LogicalPlan] {
  val catalog: Catalog
}

/**
 * This rule rewrites CTE relation definitions as sub-queries. E.g., it transforms
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
class RewriteCTEsAsSubquery(val catalog: Catalog) extends AnalysisRule {
  // Uses `transformUp` to inline all CTE relations from bottom up since inner CTE relations may
  // shadow outer CTE relations with the same names.
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case With(child, name, query, maybeAliases) =>
      child transformDown {
        case UnresolvedRelation(`name`) =>
          (maybeAliases fold query) { query rename _ } subquery name
      }
  }
}

/**
 * This rule resolves unresolved relations by looking up the table name from the `catalog`.
 */
class ResolveRelations(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case UnresolvedRelation(name) => catalog lookupRelation name
  }
}

/**
 * This rule rewrites [[Rename]] operators into [[Project projections]] to help resolve CTE queries.
 */
class RewriteRenamesToProjects(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case Resolved(child) Rename aliases Subquery subqueryName =>
      if (child.output.length >= aliases.length) {
        val aliasCount = aliases.length
        val aliased = (child.output take aliasCount, aliases).zipped map { _ as _ }
        child select (aliased ++ (child.output drop aliasCount)) subquery subqueryName
      } else {
        val expected = child.output.length
        val actual = aliases.length
        throw new AnalysisException(
          s"WITH query $subqueryName has $expected columns available but $actual columns specified"
        )
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
    case plan if plan.children.forall { _.isResolved } && !plan.isDeduplicated =>
      plan match {
        case node: Join =>
          node.copy(right = deduplicateRight(node.left, node.right))(node.metadata)

        case node: Union =>
          node.copy(right = deduplicateRight(node.left, node.right))(node.metadata)

        case node: Intersect =>
          node.copy(right = deduplicateRight(node.left, node.right))(node.metadata)

        case node: Except =>
          node.copy(right = deduplicateRight(node.left, node.right))(node.metadata)
      }
  }

  def deduplicateRight(left: LogicalPlan, right: LogicalPlan): LogicalPlan = {
    val conflictingAttributes = left.outputSet intersectByID right.outputSet

    def hasDuplicates(attributes: Set[Attribute]): Boolean =
      (attributes intersectByID conflictingAttributes).nonEmpty

    def rewriteExpressionIDs(oldPlan: LogicalPlan, newPlan: LogicalPlan): LogicalPlan = {
      val rewrite = {
        val oldOutput = oldPlan.output map { a => a: Expression }
        val newOutput = newPlan.output map { a => a: Expression }
        (oldOutput zip newOutput).toMap
      }

      right transformDown {
        case plan if plan == oldPlan => newPlan
      } transformAllExpressionsDown rewrite
    }

    right collectFirst {
      // Handles relations that introduce ambiguous attributes
      case plan: MultiInstanceRelation if hasDuplicates(plan.outputSet) =>
        plan -> plan.newInstance()

      // Handles projections that introduce ambiguous aliases
      case plan @ Project(_, projectList) if hasDuplicates(collectAliases(projectList)) =>
        plan -> plan.copy(projectList = projectList map {
          case a: Alias => a withID newExpressionID()
          case e        => e
        })(plan.metadata)
    } map {
      case (oldPlan, newPlan) => rewriteExpressionIDs(oldPlan, newPlan)
    } getOrElse right
  }

  private def collectAliases(projectList: Seq[NamedExpression]): Set[Attribute] =
    projectList.collect { case a: Alias => a.attr }.toSet
}

/**
 * This rule allows an `ORDER BY` clause to reference columns that are output of the `FROM` clause
 * but absent from the `SELECT` clause. E.g., for the following query:
 * {{{
 *   SELECT a + 1 FROM t ORDER BY a
 * }}}
 * The parsed logical plan is something like:
 * {{{
 *   Sort order=[a]
 *   +- Project projectList=[a + 1]
 *      +- Relation name=t, output=[a]
 * }}}
 * This plan tree is invalid because attribute `a` referenced by `Sort` isn't an output attribute of
 * `Project`. This rule rewrites it into:
 * {{{
 *   Project projectList=[a + 1]
 *   +- Sort order=[a]
 *      +- Project projectList=[a + 1, a]
 *         +- Relation name=t, output=[a]
 * }}}
 */
class ResolveSortReferences(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    // Ignores `Sort`s over global aggregations. They are handled separately by other aggregation
    // analysis rules.
    case plan @ Resolved(Sort(_ Project projectList, _)) if haveAggregateFunction(projectList) =>
      plan

    case Unresolved(sort @ Sort(Resolved(child Project projectList), order)) =>
      val output = projectList map { _.attr }

      val maybeResolvedOrder = order
        .map { _ tryResolveUsing output }
        .map { case e: SortOrder => e }

      val unresolvedRefs = maybeResolvedOrder
        .flatMap { _.references }
        .filterNot { _.isResolved }

      if (unresolvedRefs.isEmpty) {
        sort.copy(order = maybeResolvedOrder)(sort.metadata)
      } else {
        child
          .select((projectList ++ unresolvedRefs).distinct)
          .orderBy(maybeResolvedOrder)
          .select(output)
      }
  }
}

/**
 * This rule tries to transform all resolved logical plans operators (and expressions within them)
 * into strictly-typed form.
 *
 * @throws scraper.exceptions.AnalysisException If some resolved logical query plan operator
 *         doesn't type check.
 */
class EnforceTypeConstraints(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case Resolved(plan) => plan.strictlyTyped.get
  }
}
