package scraper.plans.logical.analysis

import scraper._
import scraper.expressions.{Alias, Attribute, AttributeRef, NamedExpression}
import scraper.expressions.NamedExpression.newExpressionID
import scraper.plans.logical._
import scraper.plans.logical.analysis.AggregationAnalysis.hasAggregateFunction
import scraper.trees.{Rule, RulesExecutor}
import scraper.trees.RulesExecutor.{FixedPoint, Once}

class Analyzer(catalog: Catalog) extends RulesExecutor[LogicalPlan] {
  override def batches: Seq[RuleBatch] = Seq(
    RuleBatch("CTE inlining", FixedPoint.Unlimited, Seq(
      new InlineCTERelationsAsSubqueries(catalog)
    )),

    RuleBatch("Resolution", FixedPoint.Unlimited, Seq(
      new ResolveRelations(catalog),
      new ResolveSortReferences(catalog),
      new DeduplicateReferences(catalog),
      new ExtractWindowFunctionsFromProjects(catalog),

      // Rules that help resolving expressions
      new ExpandStars(catalog),
      new ResolveReferences(catalog),
      new ResolveFunctions(catalog),
      new ResolveAliases(catalog),

      // Rules that help resolving aggregations
      new RewriteDistinctAggregateFunctions(catalog),
      new RewriteDistinctsAsAggregates(catalog),
      new RewriteProjectsAsGlobalAggregates(catalog),
      new AbsorbHavingConditionsIntoAggregates(catalog),
      new AbsorbSortsIntoAggregates(catalog),
      new ResolveAggregates(catalog)
    )),

    RuleBatch("Type check", Once, Seq(
      new EnforceTypeConstraints(catalog)
    )),

    RuleBatch("Post-analysis check", Once, Seq(
      new RejectUnresolvedExpressions(catalog),
      new RejectUnresolvedPlans(catalog),
      new RejectTopLevelGeneratedAttributes(catalog),
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
 * This rule inlines CTE relation definitions as sub-queries. E.g., it transforms
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
class InlineCTERelationsAsSubqueries(val catalog: Catalog) extends AnalysisRule {
  // Uses `transformUp` to inline all CTE relations from bottom up since inner CTE relations may
  // shadow outer CTE relations with the same names.
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case plan @ With(child, name, cteRelation) =>
      child transformDown {
        case UnresolvedRelation(`name`) => cteRelation subquery name
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
    case plan @ (Resolved(_ Project projectList) Sort _) if hasAggregateFunction(projectList) =>
      plan

    case Unresolved(plan @ Resolved(child Project projectList) Sort order) =>
      val orderReferences = order.flatMap(_.collect { case a: Attribute => a }).distinct
      child select (projectList ++ orderReferences).distinct orderBy order select plan.output
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
