package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.IllegalAggregationException
import scraper.expressions._
import scraper.expressions.aggregates.{AggregateFunction, DistinctAggregateFunction}
import scraper.plans.logical._
import scraper.plans.logical.analysis.AggregationAnalysis.hasAggregateFunction

/**
 * This rule rewrites `SELECT DISTINCT` into aggregation. E.g., it transforms
 * {{{
 *   SELECT DISTINCT a, b FROM t
 * }}}
 * into
 * {{{
 *   SELECT a, b FROM t GROUP BY a, b
 * }}}
 */
class RewriteDistinctsAsAggregates(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case Distinct(Resolved(child)) =>
      child groupBy child.output agg child.output
  }
}

/**
 * This rule converts [[Project]]s containing aggregate functions into unresolved global
 * aggregates, i.e., an [[UnresolvedAggregate]] without grouping keys.
 */
class GlobalAggregates(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case Resolved(child Project projectList) if hasAggregateFunction(projectList) =>
      child groupBy Nil agg projectList
  }
}

/**
 * A [[Filter]] directly over an [[UnresolvedAggregate]] corresponds to a "having condition" in
 * SQL. Having condition can only reference grouping keys and aggregated expressions, and thus
 * must be resolved together with the [[UnresolvedAggregate]] beneath it. This rule merges such
 * having conditions into [[UnresolvedAggregate]]s beneath them so that they can be resolved
 * together later.
 */
class MergeHavingConditions(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case (agg: UnresolvedAggregate) Filter condition if agg.projectList forall (_.isResolved) =>
      // Tries to resolve all unresolved attributes referencing output of the project list.
      val rewrittenCondition = {
        val partiallyResolved = condition resolveUsing (agg.projectList map (_.toAttribute))
        Alias.inlineAliases(partiallyResolved, agg.projectList)
      }

      // All having conditions should be preserved
      agg.copy(havingConditions = agg.havingConditions :+ rewrittenCondition)
  }
}

/**
 * A [[Sort]] directly over an [[UnresolvedAggregate]] is special. Its ordering expressions can
 * only reference grouping keys and aggregated expressions, and thus must be resolved together
 * with the [[UnresolvedAggregate]] beneath it. This rule merges such [[Sort]]s into
 * [[UnresolvedAggregate]]s beneath them so that they can be resolved together later.
 */
class MergeSortsOverAggregates(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case (agg: UnresolvedAggregate) Sort order if agg.projectList forall (_.isResolved) =>
      // Tries to resolve all unresolved attributes referencing output of the project list.
      val inputAttributes = agg.projectList map (_.toAttribute)
      val rewrittenOrder = order map { sortOrder =>
        val partiallyResolved = sortOrder.child resolveUsing inputAttributes
        sortOrder.copy(child = Alias.inlineAliases(partiallyResolved, agg.projectList))
      }

      // Only preserves the last sort order
      agg.copy(order = rewrittenOrder)
  }
}

class RewriteDistinctAggregateFunctions(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressionsDown {
    case _: DistinctAggregateFunction =>
      throw new UnsupportedOperationException("Distinct aggregate function is not supported yet")
  }
}

/**
 * This rule resolves an [[UnresolvedAggregate]] into an [[Aggregate]], an optional [[Filter]] if
 * there exists a `HAVING` condition, an optional [[Sort]] if there exist any sort ordering
 * expressions, plus a top-level [[Project]].
 */
class ResolveAggregates(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan =
    if (tree.collectFirst(shouldSkip).nonEmpty) {
      tree
    } else {
      tree transformDown resolveUnresolvedAggregate
    }

  // We should skip this analysis rule if the plan tree contains any of the following patterns.
  private val shouldSkip: PartialFunction[LogicalPlan, LogicalPlan] = {
    // Waits until all adjacent having conditions are merged
    case plan @ ((_: UnresolvedAggregate) Filter _) =>
      plan

    // Waits until all adjacent sorts are merged
    case plan @ ((_: UnresolvedAggregate) Sort _) =>
      plan

    // Waits until project list, having condition, and sort order expressions are all resolved
    case plan: UnresolvedAggregate if plan.expressions exists (!_.isResolved) =>
      plan
  }

  private val resolveUnresolvedAggregate: PartialFunction[LogicalPlan, LogicalPlan] = {
    case agg @ UnresolvedAggregate(Resolved(child), keys, projectList, conditions, order) =>
      // Aliases all grouping keys
      val keyAliases = keys map (GroupingAlias(_))
      val rewriteKeys = keys.zip(keyAliases.map(_.toAttribute)).toMap

      // Collects all aggregate functions
      val aggs = collectAggregateFunctions(projectList ++ conditions ++ order)

      // Checks for invalid nested aggregate functions like `MAX(COUNT(*))`
      aggs foreach rejectNestedAggregateFunction

      // Aliases all collected aggregate functions
      val aggAliases = aggs map (AggregationAlias(_))
      val rewriteAggs = (aggs: Seq[Expression]).zip(aggAliases.map(_.toAttribute)).toMap

      def rewrite(expression: Expression) = expression transformDown {
        case e => rewriteKeys orElse rewriteAggs applyOrElse (e, identity[Expression])
      }

      // Replaces grouping keys and aggregate functions in having condition, sort ordering
      // expressions, and projected named expressions.
      val rewrittenConditions = conditions map rewrite
      val rewrittenOrdering = order map (order => order.copy(child = rewrite(order.child)))
      val rewrittenProjectList = projectList map (e => rewrite(e) -> e) map {
        // Top level `GeneratedAttribute`s should be aliased to names of the original expressions
        case (g: GeneratedAttribute, e) => g as e.name
        case (e: NamedExpression, _)    => e
      }

      // At this stage, no `AttributeRef`s should appear in the following 3 rewritten expressions.
      // This is because all `AttributeRef`s in the original `UnresolvedAggregate` operator should
      // only appear in grouping keys and/or aggregate functions, which have already been rewritten
      // to `GroupingAttribute`s and `AggregationAttribute`s.
      val output = rewrittenProjectList map (_.toAttribute)
      rejectDanglingAttributes("SELECT field", keys, Nil, rewrittenProjectList)
      rejectDanglingAttributes("HAVING condition", keys, output, rewrittenConditions)
      rejectDanglingAttributes("ORDER BY expression", keys, output, rewrittenOrdering)

      child
        .resolvedGroupBy(keyAliases)
        .agg(aggAliases)
        .filterOption(rewrittenConditions)
        .orderByOption(rewrittenOrdering)
        .select(rewrittenProjectList)
  }

  private def collectAggregateFunctions(expressions: Seq[Expression]): Seq[AggregateFunction] = {
    // `DistinctAggregateFunction`s must be collected first. Otherwise, their child expressions,
    // which are also `AggregateFunction`s, will be collected unexpectedly.
    val distinctAggs = expressions.flatMap(_ collect {
      case a: DistinctAggregateFunction => a
    }).distinct

    val aggs = expressions.map(_ transformDown {
      // Eliminates previously collected `DistinctAggregateFunction`s first...
      case a: DistinctAggregateFunction => AggregationAlias(a).toAttribute
    }).flatMap(_ collect {
      // ... and then collects all non-distinct aggregate functions.
      case a: AggregateFunction => a
    }).distinct

    distinctAggs ++ aggs
  }

  private def rejectNestedAggregateFunction(agg: AggregateFunction): Unit = agg match {
    case DistinctAggregateFunction(child) =>
      // Special cases `DistinctAggregateFunction` since it always has another aggregate function as
      // child expression.
      rejectNestedAggregateFunction(child)

    case _ =>
      agg.children.foreach(_ collectFirst {
        case nested: AggregateFunction =>
          throw new IllegalAggregationException(agg, nested)
      })
  }

  private def rejectDanglingAttributes(
    part: String, keys: Seq[Expression], output: Seq[Attribute], expressions: Seq[Expression]
  ): Unit = expressions foreach { e =>
    e.references collectFirst {
      case a: AttributeRef if !output.contains(a) => a
    } foreach { a =>
      throw new IllegalAggregationException(part, a, e, keys)
    }
  }
}

object AggregationAnalysis {
  private[analysis] def hasAggregateFunction(expressions: Seq[Expression]): Boolean =
    expressions exists (_.collectFirst { case _: AggregateFunction => () }.nonEmpty)
}
