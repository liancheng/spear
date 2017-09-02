package spear.plans.logical.analysis

import spear._
import spear.exceptions.IllegalAggregationException
import spear.expressions._
import spear.expressions.aggregates.{AggregateFunction, DistinctAggregateFunction}
import spear.expressions.InternalAlias._
import spear.expressions.windows.WindowFunction
import spear.plans.logical._
import spear.plans.logical.analysis.AggregationAnalysis._
import spear.plans.logical.analysis.WindowAnalysis._
import spear.plans.logical.patterns.Resolved
import spear.utils._

/**
 * This rule rewrites a distinct projection into aggregations. E.g., it transforms SQL query
 * {{{
 *   SELECT DISTINCT a, b FROM t
 * }}}
 * into
 * {{{
 *   SELECT a, b FROM t GROUP BY a, b
 * }}}
 */
class RewriteDistinctToAggregate(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case Distinct(Resolved(child)) =>
      child groupBy child.output agg child.output
  }
}

/**
 * This rule converts a [[Project]] containing aggregate functions into a global aggregate, i.e. an
 * [[UnresolvedAggregate]] without any grouping keys.
 */
class RewriteProjectToGlobalAggregate(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case Resolved(child Project projectList) if hasAggregateFunction(projectList) =>
      child groupBy Nil agg projectList
  }
}

/**
 * This rule matches adjacent [[Filter]] and/or [[Sort]] operators above [[UnresolvedAggregate]]
 * operators and merges the filter condition and sort order expressions into the underlying
 * [[UnresolvedAggregate]] operator.
 *
 * This transformation is necessary because the [[Filter]] and [[Sort]] operators may contain or
 * reference grouping keys and/or aggregate functions that cannot be resolved directly.
 *
 * For example, assuming table `t` has columns `x`, `y`, and `z`, then the following SQL statement:
 * {{{
 *   SELECT count(x) AS c
 *   FROM t
 *   GROUP BY y
 *   HAVING max(z) > 0
 *   ORDER BY y DESC
 * }}}
 * can be parsed into the following logical plan:
 * {{{
 *   Sort order=[y DESC] => [c]                                                 (1)
 *   +- Filter condition=max(z) > 0 => [c]                                      (2)
 *      +- UnresolvedAggregate keys=[y], projectList=[count(x) AS c] => [c]     (3)
 *         +- Relation name=t => [x, y, z]
 * }}}
 * The bracketed lists after the `=>` are output attributes of the corresponding logical plan
 * operator.
 *
 * Note that neither `y` referenced in (1) nor `z` referenced in (2) is an output attribute of the
 * [[UnresolvedAggregate]] in (3). Therefore, neither the [[Sort]] nor the [[Filter]] operator can
 * be directly resolved separately.
 *
 * To solve this issue, this rule rewrites the above plan into:
 * {{{
 *   UnresolvedAggregate keys=[\$0], projectList=[\$1], condition=[\$2], order=[\$3] => [c]
 *   | |- \$0: y
 *   | |- \$1: count(x) AS c
 *   | |- \$2: max(z) > 0
 *   | +- \$3: y DESC
 *   +- Relation name=t => [x, y, z]
 * }}}
 * Now, references to attributes `y` and `z` are moved into the [[UnresolvedAggregate]], and can be
 * easily resolved by the [[ResolveReference]] rule since they are now among the output attribute
 * lists of the relation `t`, which lives right beneath the [[UnresolvedAggregate]] operator.
 *
 * @see [[UnresolvedAggregate]]
 * @see [[RewriteUnresolvedAggregate]]
 */
class UnifyFilteredSortedAggregate(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case (agg: UnresolvedAggregate) Filter condition if agg.projectList forall { _.isResolved } =>
      // All having conditions should be preserved.
      agg.copy(conditions = agg.conditions :+ condition)

    case (agg: UnresolvedAggregate) Sort order if agg.projectList forall { _.isResolved } =>
      // Unaliases all aliases that are introduced by the `UnresolvedAggregate` underneath, and
      // referenced by some sort order expression(s).
      val unaliased = order
        .map { _ tryResolveUsing agg.projectList }
        .map { _ unaliasUsing (agg.projectList, SortOrderNamespace) }
        .map { case e: SortOrder => e }

      // Only preserves the last sort order.
      agg.copy(order = unaliased)
  }
}

class RewriteDistinctAggregateFunction(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case node =>
      node transformExpressionsDown {
        case _: DistinctAggregateFunction =>
          throw new UnsupportedOperationException(
            "Distinct aggregate functions are not supported yet"
          )
      }
  }
}

/**
 * This rule rewrites an [[UnresolvedAggregate]] into a combination of the following operators:
 *
 *  - an [[Aggregate]] that evaluates non-window aggregate function found in `SELECT`, `HAVING`,
 *    and/or `ORDER BY` clauses, and
 *  - an optional [[Filter]] that corresponds to the `HAVING` clause, and
 *  - zero or more [[Window]]s that are responsible for evaluating window functions found in
 *    `SELECT` and/or `ORDER BY` clauses, and
 *  - an optional [[Sort]] that corresponds to the `ORDER BY` clause, and
 *  - a top-level [[Project]] that is used to evaluate non-aggregate and non-window expressions and
 *    assemble the final output attributes.
 *
 * These operators are stacked over each other to form the following structure:
 * {{{
 *   Project projectList=[<output-expressions>]
 *   +- Sort order=[<sort-orders>]
 *      +- Window functions=[<window-functions-w/-window-spec-n>]
 *         +- ...
 *            +- Window functions=[<window-functions-w/-window-spec-1>]
 *               +- Window functions=[<window-functions-w/-window-spec-0>]
 *                  +- Filter condition=<having-condition>
 *                     +- Aggregate keys=[<grouping-keys>] functions=[<agg-functions>]
 *                        +- <child plan>
 * }}}
 */
class RewriteUnresolvedAggregate(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan =
    tree collectFirstDown skip map { _ => tree } getOrElse { tree transformUp rewriter }

  // This partial function plays the role of a guard that ensures all the pre-conditions of this
  // analysis rule. We should skip this rule by returning the original query plan whenever the plan
  // tree contains any of the following patterns.
  private val skip: PartialFunction[LogicalPlan, Unit] = {
    // Waits until all adjacent having conditions are absorbed.
    case (_: UnresolvedAggregate) Filter _ =>

    // Waits until all adjacent sorts are absorbed.
    case (_: UnresolvedAggregate) Sort _ =>

    // Waits until project list, having condition, and sort order expressions are all resolved.
    case plan: UnresolvedAggregate if plan.expressions exists { !_.isResolved } =>

    // Waits until all distinct aggregate functions are rewritten into normal aggregate functions.
    case plan: UnresolvedAggregate if hasDistinctAggregateFunction(plan.projectList) =>

    // Rejects aggregate functions in grouping keys.
    case plan: UnresolvedAggregate if hasAggregateFunction(plan.keys) =>
      plan.keys foreach { key =>
        val aggs = collectAggregateFunctions(key)

        if (aggs.nonEmpty) {
          throw new IllegalAggregationException(
            s"""Aggregate functions are not allowed in grouping keys:
               |
               | - aggregate function found: ${aggs.head.sqlLike}
               | - grouping key: ${key.sqlLike}
               |""".stripMargin
          )
        }
      }

    // Window functions are not allowed in grouping keys or HAVING conditions.
    case plan: UnresolvedAggregate if hasWindowFunction(plan.keys ++ plan.conditions) =>
      def rejectIllegalWindowFunctions(component: String)(e: Expression): Unit = {
        val wins = collectWindowFunctions(e)

        if (wins.nonEmpty) {
          throw new IllegalAggregationException(
            s"""Window functions are not allowed in $component:
               |
               | - window function found: ${wins.head.sqlLike}
               | - $component: ${e.sqlLike}
               |""".stripMargin
          )
        }
      }

      plan.keys foreach rejectIllegalWindowFunctions("grouping key")
      plan.conditions foreach rejectIllegalWindowFunctions("HAVING condition")
  }

  private val rewriter: PartialFunction[LogicalPlan, LogicalPlan] = {
    case UnresolvedAggregate(Resolved(child), keys, projectList, conditions, order) =>
      val keyAliases = keys map { GroupingKeyAlias(_) }
      logInternalAliases(keyAliases, "grouping keys")

      val rewriteKeys = (_: Expression) transformUp buildRewriter(keyAliases)
      val restoreKeys = (_: Expression) transformUp buildRestorer(keyAliases)

      val aggs = collectAggregateFunctions(projectList ++ conditions ++ order map rewriteKeys)
      aggs foreach rejectNestedAggregateFunction

      val aggAliases = aggs map { AggregateFunctionAlias(_) }
      logInternalAliases(aggAliases, "aggregate functions")

      val aggRewriter = buildRewriter(aggAliases)
      val restoreAggs = (_: Expression) transformUp buildRestorer(aggAliases)
      val rewriteAggs = (_: Expression) transformUp aggRewriter transformUp {
        // Window aggregate functions should not be rewritten. Restores them here. E.g.:
        //
        //  - SELECT max(a) OVER (PARTITION BY max(a)), max(a) FROM t GROUP BY a
        //
        //    The 2nd and 3rd `max(a)` should be rewritten while the 1st one must be preserved.
        //
        //  - SELECT max(avg(b)) OVER () FROM t GROUP BY a
        //
        //    The nested `avg(b)` should be rewritten while the outer `max` should be preserved.
        case e @ WindowFunction(f, _) =>
          e.copy(function = restoreAggs(f) transformChildrenUp aggRewriter)
      }

      // Note: window functions may appear in both SELECT and ORDER BY clauses.
      val wins = collectWindowFunctions(projectList ++ order map rewriteKeys.andThen(rewriteAggs))
      val winAliases = wins map { WindowFunctionAlias(_) }
      logInternalAliases(winAliases, "window functions")

      val rewriteWins = (_: Expression) transformUp buildRewriter(winAliases)
      val restoreWins = (_: Expression) transformUp buildRestorer(winAliases)

      val rewrite = rewriteKeys andThen rewriteAggs andThen rewriteWins
      val restore = restoreKeys compose restoreAggs compose restoreWins

      val rewrittenConditions = conditions map rewrite
      val rewrittenOrder = order map rewrite
      val rewrittenProjectList = projectList map { named =>
        // When rewriting the outermost project list, no `InternalAttribute`s should be exposed
        // outside. Here we alias them using names and expression IDs of the original named
        // expressions.
        rewrite(named) match {
          case e: AttributeRef if e.namespace.nonEmpty => e as named.name withID named.expressionID
          case e: NamedExpression                      => e
        }
      }

      def rejectIllegalRefs(component: String, whitelist: Seq[Attribute] = Nil)(e: Expression) =
        e.references collectFirst {
          case a: AttributeRef if a.namespace.isEmpty && !(whitelist contains a) =>
            val keyList = keys map { _.sqlLike } mkString ("[", ", ", "]")
            throw new IllegalAggregationException(
              s"""Attribute ${a.sqlLike} in $component ${restore(e).sqlLike} is neither referenced
                 |by a non-window aggregate function nor a grouping key among $keyList
                 |""".oneLine
            )
        }

      wins foreach rejectIllegalRefs("window function")
      rewrittenProjectList foreach rejectIllegalRefs("SELECT field")

      // The `HAVING` clause and the `ORDER BY` clause are allowed to reference output attributes
      // produced by the `SELECT` clause.
      val output = rewrittenProjectList map { _.attr }
      rewrittenConditions foreach rejectIllegalRefs("HAVING condition", output)
      rewrittenOrder foreach rejectIllegalRefs("ORDER BY expression", output)

      child
        .aggregate(keyAliases, aggAliases)
        .filter(rewrittenConditions)
        .windows(winAliases)
        .sort(rewrittenOrder)
        .select(rewrittenProjectList)
  }

  private def logInternalAliases(aliases: Seq[InternalAlias], collectionName: String): Unit =
    if (aliases.nonEmpty) {
      val aliasList = aliases map { alias =>
        s"  - ${alias.child.sqlLike} -> ${alias.attr.debugString}"
      } mkString "\n"

      logDebug(
        s"""Collected $collectionName:
           |
           |$aliasList
           |""".stripMargin
      )
    }

  private def rejectNestedAggregateFunction(agg: AggregateFunction): Unit = agg match {
    case DistinctAggregateFunction(child) =>
      // Special cases `DistinctAggregateFunction` since it always has another aggregate function as
      // child expression.
      rejectNestedAggregateFunction(child)

    case e =>
      e.children foreach {
        _ collectFirstDown {
          case _: AggregateFunction =>
            throw new IllegalAggregationException(
              "Aggregate function can't be nested within another aggregate function: " + e.sqlLike
            )
        }
      }
  }

  private def hasDistinctAggregateFunction(expressions: Seq[Expression]): Boolean =
    expressions exists hasDistinctAggregateFunction

  private def hasDistinctAggregateFunction(expression: Expression): Boolean =
    eliminateWindowFunctions(expression).collectFirstDown {
      case _: DistinctAggregateFunction =>
    }.nonEmpty
}

object AggregationAnalysis {
  def hasAggregateFunction(expressions: Seq[Expression]): Boolean =
    collectAggregateFunctions(expressions).nonEmpty

  def hasAggregateFunction(expression: Expression): Boolean =
    collectAggregateFunctions(expression).nonEmpty

  /**
   * Collects all non-window aggregate functions from the given `expressions`.
   */
  def collectAggregateFunctions(expressions: Seq[Expression]): Seq[AggregateFunction] =
    expressions.flatMap(collectAggregateFunctions).distinct

  /**
   * Collects all non-window aggregate functions from the given `expression`.
   */
  def collectAggregateFunctions(expression: Expression): Seq[AggregateFunction] = {
    val collectDistinctAggs = (_: Expression) collectDown {
      case e: DistinctAggregateFunction => e: AggregateFunction
    }

    val collectAggs = (_: Expression) collectDown {
      case e: AggregateFunction => e
    }

    val eliminateDistinctAggs = (_: Expression) transformDown {
      case e: DistinctAggregateFunction => AggregateFunctionAlias(e).attr
    }

    // Finds out all window functions within the given expression and collects all *non-window*
    // aggregate functions inside these window functions recursively. Take the following window
    // expression as an example:
    //
    //   max(count(a)) OVER (PARTITION BY avg(b) ORDER BY sum(c))
    //       ~~~~~~~~                     ~~~~~~          ~~~~~~
    //
    // We should collect non-window aggregate functions `count(a)`, `avg(b)`, and `sum(c)` but not
    // the window aggregate function `max(count(a))`.
    val aggsInsideWindowFunctions = for {
      WindowFunction(f, spec) <- collectWindowFunctions(expression)
      child <- f.children :+ spec
      agg <- collectAggregateFunctions(child)
    } yield agg

    // Collects all distinct and non-distinct aggregate functions outside any window functions.
    val aggsOutsideWindowFunctions = {
      val windowFunctionsEliminated = eliminateWindowFunctions(expression)
      val distinctAggs = collectDistinctAggs(windowFunctionsEliminated)
      val regularAggs = collectAggs(eliminateDistinctAggs(windowFunctionsEliminated))
      distinctAggs ++ regularAggs
    }

    (aggsInsideWindowFunctions ++ aggsOutsideWindowFunctions).distinct
  }

  def eliminateWindowFunctions(expression: Expression): Expression = expression transformDown {
    case e: WindowFunction => WindowFunctionAlias(e).attr
  }
}
