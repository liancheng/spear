package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.IllegalAggregationException
import scraper.expressions._
import scraper.expressions.aggregates.{AggregateFunction, DistinctAggregateFunction}
import scraper.expressions.windows.WindowFunction
import scraper.plans.logical._
import scraper.plans.logical.analysis.AggregationAnalysis._
import scraper.plans.logical.analysis.WindowAnalysis._

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
class RewriteProjectsAsGlobalAggregates(val catalog: Catalog) extends AnalysisRule {
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
class AbsorbHavingConditionsIntoAggregates(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case (agg: UnresolvedAggregate) Filter condition if agg.projectList forall (_.isResolved) =>
      // Tries to resolve all unresolved attributes referencing output of the project list.
      val rewrittenCondition = Alias.inlineAliases(
        condition resolveUsing agg.projectList,
        agg.projectList
      )

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
class AbsorbSortsIntoAggregates(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case (agg: UnresolvedAggregate) Sort order if agg.projectList forall (_.isResolved) =>
      // Tries to resolve all unresolved attributes referencing output of the project list.
      val rewrittenOrder = order map { sortOrder =>
        sortOrder.copy(child = Alias.inlineAliases(
          sortOrder.child resolveUsing agg.projectList,
          agg.projectList
        ))
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
 * expressions, zero or more [[Window]] if there exists window function(s), plus a top-level
 * [[Project]].
 */
class ResolveAggregates(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan =
    tree collectFirst shouldSkip map (_ => tree) getOrElse {
      tree transformDown resolveUnresolvedAggregate
    }

  // We should skip this analysis rule if the plan tree contains any of the following patterns.
  private val shouldSkip: PartialFunction[LogicalPlan, LogicalPlan] = {
    // Waits until all adjacent having conditions are absorbed.
    case plan @ ((_: UnresolvedAggregate) Filter _) =>
      plan

    // Waits until all adjacent sorts are absorbed.
    case plan @ ((_: UnresolvedAggregate) Sort _) =>
      plan

    // Waits until project list, having condition, and sort order expressions are all resolved.
    case plan: UnresolvedAggregate if plan.expressions exists (!_.isResolved) =>
      plan

    // Waits until all distinct aggregate functions are rewritten into normal aggregate functions.
    case plan: UnresolvedAggregate if hasDistinctAggregateFunction(plan.projectList) =>
      plan
  }

  private val resolveUnresolvedAggregate: PartialFunction[LogicalPlan, LogicalPlan] = {
    case agg @ UnresolvedAggregate(Resolved(child), keys, projectList, conditions, order) =>
      // Aliases all grouping keys.
      val keyAliases = keys map (GroupingAlias(_))

      // Collects and aliases all aggregate functions.
      val aggs = collectAggregateFunctions(projectList ++ conditions ++ order)
      val aggAliases = aggs map (AggregationAlias(_))

      // Checks for invalid nested aggregate functions like `MAX(COUNT(*))`.
      aggs foreach rejectNestedAggregateFunction

      // Collects and aliases all window functions.
      val wins = collectWindowFunctions(projectList)
      val winAliases = wins map (WindowAlias(_))

      def buildRewriter(aliases: Seq[GeneratedAlias]): Map[Expression, Expression] =
        aliases.map { a => a.child -> (a.attr: Expression) }.toMap

      val rewriteKeys = buildRewriter(keyAliases)
      val rewriteAggs = buildRewriter(aggAliases)
      val rewriteWins = buildRewriter(winAliases)

      // Used to restore the original expressions for error reporting.
      val restoreKeys = rewriteKeys map (_.swap)
      val restoreAggs = rewriteAggs map (_.swap)
      val restoreWins = rewriteWins map (_.swap)

      // While being used in an aggregation, the only input attributes a window function can
      // reference are grouping keys. E.g., these queries are invalid:
      //
      //   SELECT max(a) OVER (...) FROM t GROUP BY a
      //   SELECT sum(a % 10) OVER (...) FROM t GROUP BY a % 10
      //
      // while these are not:
      //
      //   SELECT max(b) OVER (...) FROM t GROUP BY a
      //   SELECT sum(a) OVER (...) FROM t GROUP BY a % 10
      //
      // Therefore, after rewriting all grouping keys to their corresponding `GroupingAttribute`s,
      // no other input attributes should be found in the rewritten window functions anymore.
      rejectDanglingAttributes(
        "window function", wins.map(_ transformDown rewriteKeys), keys, Nil, restoreKeys
      )

      val rewrite = (_: Expression) transformDown {
        rewriteKeys orElse rewriteAggs orElse rewriteWins
      }

      // Rewrites grouping keys, aggregate functions, and window functions appearing in having
      // condition, sort ordering expressions, and projected named expressions to corresponding
      // `GroupingAttribute`s, `AggregationAttribute`s, and `WindowAttribute`s.
      val rewrittenConditions = conditions map rewrite
      val rewrittenOrder = order map (ord => ord.copy(child = rewrite(ord.child)))
      val rewrittenProjectList = projectList map rewrite zip projectList map {
        // Top level `GeneratedAttribute`s should be aliased with names and expression IDs of the
        // original named expressions so that the transformed plan still has the same output
        // attributes as the original plan.
        case (g: GeneratedAttribute, e) => g as e.name withID e.expressionID
        case (e: NamedExpression, _)    => e
      }

      // Expressions appearing in aggregation must conform to the following constraints:
      //
      //  1. The only input attributes a projected named expression can reference are
      //
      //     - grouping keys, and
      //     - attributes referenced by non-window aggregate functions.
      //
      //     E.g., these queries are valid:
      //
      //       SELECT a + 1 FROM t GROUP BY a + 1
      //       SELECT a + 1 + 1 FROM t GROUP BY a + 1
      //       SELECT max(b) FROM t GROUP BY a + 1
      //
      //     while these are not:
      //
      //       SELECT a + 2 FROM t GROUP BY a + 1
      //       SELECT max(b) OVER (...) FROM t GROUP BY a + 1
      //
      //  2. The only input attributes a having condition or a sort ordering expression can
      //     reference are
      //
      //     - grouping keys,
      //     - attributes referenced by non-window aggregate functions, and
      //     - output attribute of the project list.
      //
      //     E.g., these queries are valid:
      //
      //       SELECT max(b) AS max FROM t GROUP BY a + 1 HAVING sum(a) < 10
      //       SELECT max(b) AS max FROM t GROUP BY a + 1 ORDER BY a + 1 DESC
      //       SELECT max(b) AS max FROM t GROUP BY a + 1 HAVING max > 0
      //
      //     while these are not:
      //
      //       SELECT max(b) AS max FROM t GROUP BY a + 1 HAVING a + 2 < 10
      //       SELECT max(b) AS max FROM t GROUP BY a + 1 ORDER BY b DESC
      //
      // At this stage, we've already rewritten all grouping keys and aggregate functions to
      // corresponding `GroupingAttribute`s and `AggregationAttribute`s. Therefore, no other input
      // attributes should be found in these rewritten expressions anymore.
      val output = rewrittenProjectList map (_.toAttribute)
      val restore = restoreKeys orElse restoreAggs orElse restoreWins
      rejectDanglingAttributes("SELECT field", rewrittenProjectList, keys, Nil, restore)
      rejectDanglingAttributes("HAVING condition", rewrittenConditions, keys, output, restore)
      rejectDanglingAttributes("ORDER BY expression", rewrittenOrder, keys, output, restore)

      child
        .resolvedGroupBy(keyAliases)
        .agg(aggAliases)
        .filterOption(rewrittenConditions)
        .orderByOption(rewrittenOrder)
        .windowsOption(winAliases)
        .select(rewrittenProjectList)
  }

  // Pre-condition: `expressions` contain no `DistinctAggregateFunction`.
  private def collectAggregateFunctions(expressions: Seq[Expression]): Seq[AggregateFunction] = {
    // Windowed aggregate functions should be eliminated first since they are handled separately by
    // the `Window` operator.
    val windowFunctionsEliminated = expressions map (_ transformDown {
      case e: WindowFunction => WindowAlias(e).toAttribute
    })

    windowFunctionsEliminated.flatMap(_ collect {
      case a: AggregateFunction => a
    }).distinct
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
    kind: String,
    expressions: Seq[Expression],
    groupingKeys: Seq[Expression],
    outputAttributes: Seq[Attribute],
    restore: PartialFunction[Expression, Expression]
  ): Unit = expressions foreach { e =>
    e.references collectFirst {
      case a: AttributeRef if !(outputAttributes contains a) =>
        throw new IllegalAggregationException(kind, a, e transformDown restore, groupingKeys)
    }
  }

  private def hasDistinctAggregateFunction(expressions: Seq[Expression]): Boolean =
    expressions exists hasDistinctAggregateFunction

  private def hasDistinctAggregateFunction(expression: Expression): Boolean =
    expression.transformDown {
      // Excludes aggregate functions in window functions
      case e: WindowFunction => WindowAlias(e).toAttribute
    }.collectFirst {
      case e: DistinctAggregateFunction => ()
    }.nonEmpty
}

object AggregationAnalysis {
  private[analysis] def hasAggregateFunction(expressions: Seq[Expression]): Boolean =
    expressions exists hasAggregateFunction

  private[analysis] def hasAggregateFunction(expression: Expression): Boolean =
    expression.transformDown {
      // Excludes aggregate functions in window functions
      case e: WindowFunction => WindowAlias(e).toAttribute
    }.collectFirst {
      case e: AggregateFunction => ()
    }.nonEmpty
}
