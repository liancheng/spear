package scraper.plans.logical.analysis

import scraper._
import scraper.plans.logical._
import scraper.trees.{Rule, RulesExecutor}
import scraper.trees.RulesExecutor.{FixedPoint, Once}

class Analyzer(catalog: Catalog) extends RulesExecutor[LogicalPlan] {
  override def batches: Seq[RuleBatch] = Seq(
    RuleBatch("CTE inlining", FixedPoint.Unlimited, Seq(
      new InlineCTERelationsAsSubqueries(catalog)
    )),

    RuleBatch("Resolution", FixedPoint.Unlimited, Seq(
      new ResolveRelations(catalog),

      // Rules that help resolving expressions
      new ResolveFunctions(catalog),
      new ExpandStars(catalog),
      new ResolveReferences(catalog),
      new ResolveAliases(catalog),
      new DeduplicateReferences(catalog),

      // Rules that help resolving aggregations
      new RewriteDistinctAggregateFunctions(catalog),
      new ResolveSortReferences(catalog),
      new RewriteDistinctsAsAggregates(catalog),
      new GlobalAggregates(catalog),
      new MergeHavingConditions(catalog),
      new MergeSortsOverAggregates(catalog),
      new ResolveAggregates(catalog)
    )),

    RuleBatch("Type check", Once, Seq(
      new EnforceTypeConstraints(catalog)
    )),

    RuleBatch("Post-analysis check", Once, Seq(
      new RejectUnresolvedExpressions(catalog),
      new RejectUnresolvedPlans(catalog),
      new RejectGeneratedAttributes(catalog),
      new RejectDistinctAggregateFunctions(catalog)
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
