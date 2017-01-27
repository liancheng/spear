package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.IllegalAggregationException
import scraper.expressions._
import scraper.expressions.aggregates.{AggregateFunction, DistinctAggregateFunction}
import scraper.expressions.InternalAlias.{buildRestorer, buildRewriter}
import scraper.expressions.windows.WindowFunction
import scraper.plans.logical._
import scraper.plans.logical.analysis.AggregationAnalysis._
import scraper.plans.logical.analysis.WindowAnalysis._
import scraper.utils._

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
class RewriteDistinctsAsAggregates(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case Distinct(Resolved(child)) =>
      child groupBy child.output agg child.output
  }
}

/**
 * This rule converts a [[Project]] containing aggregate functions into a global aggregate, i.e. an
 * [[UnresolvedAggregate]] without any grouping keys.
 */
class RewriteProjectsAsGlobalAggregates(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case Resolved(child Project projectList) if hasAggregateFunction(projectList) =>
      child groupBy Nil agg projectList
  }
}

/**
 * A [[Filter]] directly over an [[UnresolvedAggregate]] corresponds to a `HAVING` clause. Its
 * predicate must be resolved together with the [[UnresolvedAggregate]] because it may refer to
 * grouping keys and aggregate functions. This rule extracts the predicate expression of such a
 * [[Filter]] and merges the predicate into the [[UnresolvedAggregate]] underneath.
 *
 * @see [[RewriteUnresolvedAggregates]]
 */
class AbsorbHavingConditionsIntoAggregates(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case (agg: UnresolvedAggregate) Filter condition if agg.projectList forall (_.isResolved) =>
      // Tries to resolve and unalias all unresolved attributes using project list output.
      val rewrittenCondition = tryResolveAndUnalias(agg.projectList)(condition)

      // `HAVING` predicates are always evaluated before window functions, therefore `HAVING`
      // predicates must not reference any (aliases of) window functions.
      rewrittenCondition transformUp {
        case _: WindowFunction | _: WindowAttribute =>
          throw new IllegalAggregationException(
            "Window functions are not allowed in HAVING clauses."
          )
      }

      // All having conditions should be preserved.
      agg.copy(conditions = agg.conditions :+ rewrittenCondition)
  }
}

/**
 * A [[Sort]] directly over an [[UnresolvedAggregate]] is special, its sort ordering expressions
 * must be resolved together with the [[UnresolvedAggregate]] since it may refer to grouping keys
 * and aggregate functions. This rule extracts sort ordering expressions of such a [[Sort]] and
 * merges them into the [[UnresolvedAggregate]] underneath.
 *
 * @see [[RewriteUnresolvedAggregates]]
 */
class AbsorbSortsIntoAggregates(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case (agg: UnresolvedAggregate) Sort order if agg.projectList forall (_.isResolved) =>
      // Only preserves the last sort order.
      agg.copy(order = order map tryResolveAndUnalias(agg.projectList))
  }
}

class RewriteDistinctAggregateFunctions(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressionsDown {
    case _: DistinctAggregateFunction =>
      throw new UnsupportedOperationException("Distinct aggregate function is not supported yet")
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
class RewriteUnresolvedAggregates(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan =
    // Only executes this rule when all the pre-conditions hold.
    tree collectFirst preConditionViolations map { _ => tree } getOrElse {
      tree transformDown rewrite
    }

  // This partial function performs as a guard, who ensures all the pre-conditions of this analysis
  // rule. We should skip this rule by returning the original query plan whenever the plan tree
  // contains any of the following patterns.
  private val preConditionViolations: PartialFunction[LogicalPlan, Unit] = {
    // Waits until all adjacent having conditions are absorbed.
    case ((_: UnresolvedAggregate) Filter _) =>

    // Waits until all adjacent sorts are absorbed.
    case ((_: UnresolvedAggregate) Sort _) =>

    // Waits until project list, having condition, and sort order expressions are all resolved.
    case plan: UnresolvedAggregate if plan.expressions exists (!_.isResolved) =>

    // Waits until all distinct aggregate functions are rewritten into normal aggregate functions.
    case plan: UnresolvedAggregate if hasDistinctAggregateFunction(plan.projectList) =>
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

  private val rewrite: PartialFunction[LogicalPlan, LogicalPlan] = {
    case UnresolvedAggregate(Resolved(child), keys, projectList, conditions, order) =>
      val keyAliases = keys map { GroupingAlias(_) }
      logInternalAliases(keyAliases, "grouping keys")

      val rewriteKeys = (_: Expression) transformUp buildRewriter(keyAliases)
      val restoreKeys = (_: Expression) transformUp buildRestorer(keyAliases)

      val aggs = collectAggregateFunctions(projectList ++ conditions ++ order)
      aggs foreach rejectNestedAggregateFunction

      val aggAliases = aggs map { AggregationAlias(_) }
      logInternalAliases(aggAliases, "aggregate functions")

      val aggRewriter = buildRewriter(aggAliases)
      val restoreAggs = (_: Expression) transformUp buildRestorer(aggAliases)
      val rewriteAggs = (_: Expression) transformUp aggRewriter transformUp {
        // Window aggregate functions should not be rewritten. Restores them here. E.g.:
        //
        //  - SELECT max(a) OVER (PARTITION BY max(a)) FROM t GROUP BY a
        //
        //    The 2nd `max(a)` should be rewritten while the 1st one must be preserved.
        //
        //  - SELECT max(avg(b)) OVER () FROM t GROUP BY a
        //
        //    The nested `avg(b)` should be rewritten while the outer `max` should be preserved.
        case e @ WindowFunction(f, _) =>
          e.copy(function = restoreAggs(f) transformChildrenUp aggRewriter)
      }

      // Note: window functions may appear in both SELECT and ORDER BY clauses.
      val wins = collectWindowFunctions(projectList ++ order map (rewriteAggs andThen rewriteKeys))
      val winAliases = wins map { WindowAlias(_) }
      logInternalAliases(winAliases, "window functions")

      val rewriteWins = (_: Expression) transformUp buildRewriter(winAliases)
      val restoreWins = (_: Expression) transformUp buildRestorer(winAliases)

      val rewrite = rewriteAggs andThen rewriteKeys andThen rewriteWins
      val restore = restoreAggs compose restoreKeys compose restoreWins

      // When rewriting the outermost project list, no `InternalAttribute`s should be exposed
      // outside. This method aliases them using names and expression IDs of the original named
      // expressions.
      def rewriteNamedExpression(named: NamedExpression): NamedExpression = rewrite(named) match {
        case e: InternalAttribute => e as named.name withID named.expressionID
        case e: NamedExpression   => e
      }

      val rewrittenProjectList = projectList map rewriteNamedExpression
      val rewrittenConditions = conditions map rewrite
      val rewrittenOrder = order map rewrite

      def rejectOrphanReferences(
        component: String, whitelist: Seq[Attribute] = Nil
      )(e: Expression) = e.references collectFirst {
        case a: AttributeRef if !(whitelist contains a) =>
          val keyList = keys map { _.sqlLike } mkString ("[", ", ", "]")
          throw new IllegalAggregationException(
            s"""Attribute ${a.sqlLike} in $component ${restore(e).sqlLike} is neither referenced
               |by a non-window aggregate function nor a grouping key among $keyList
               |""".oneLine
          )
      }

      val output = rewrittenProjectList map { _.attr }
      wins foreach rejectOrphanReferences("window function")
      rewrittenProjectList foreach rejectOrphanReferences("SELECT field")

      // The `HAVING` clause and the `ORDER BY` clause are allowed to reference output attributes
      // produced by the `SELECT` clause.
      rewrittenConditions foreach rejectOrphanReferences("HAVING condition", output)
      rewrittenOrder foreach rejectOrphanReferences("ORDER BY expression", output)

      child
        .resolvedGroupBy(keyAliases)
        .agg(aggAliases)
        .filterOption(rewrittenConditions)
        .windowsOption(winAliases)
        .orderByOption(rewrittenOrder)
        .select(rewrittenProjectList)
  }

  private def rejectNestedAggregateFunction(agg: AggregateFunction): Unit = agg match {
    case DistinctAggregateFunction(child) =>
      // Special cases `DistinctAggregateFunction` since it always has another aggregate function as
      // child expression.
      rejectNestedAggregateFunction(child)

    case e =>
      e.children foreach (_ collectFirst {
        case _: AggregateFunction =>
          throw new IllegalAggregationException(
            "Aggregate function can't be nested within another aggregate function: " + e.sqlLike
          )
      })
  }

  private def hasDistinctAggregateFunction(expressions: Seq[Expression]): Boolean =
    expressions exists hasDistinctAggregateFunction

  private def hasDistinctAggregateFunction(expression: Expression): Boolean =
    eliminateWindowFunctions(expression).collectFirst {
      case _: DistinctAggregateFunction =>
    }.nonEmpty
}

object AggregationAnalysis {
  def hasAggregateFunction(expressions: Seq[Expression]): Boolean =
    collectAggregateFunctions(expressions).nonEmpty

  def tryResolveAndUnalias[E <: Expression](input: Seq[NamedExpression]): E => E =
    Expression.tryResolve[E](input) _ andThen Alias.unalias[E](input)

  /**
   * Collects all non-window aggregate functions from the given `expressions`.
   */
  def collectAggregateFunctions(expressions: Seq[Expression]): Seq[AggregateFunction] =
    expressions.flatMap(collectAggregateFunctions).distinct

  /**
   * Collects all non-window aggregate functions from the given `expression`.
   */
  def collectAggregateFunctions(expression: Expression): Seq[AggregateFunction] = {
    val collectDistinctAggs = (_: Expression) collect {
      case e: DistinctAggregateFunction => e: AggregateFunction
    }

    val collectAggs = (_: Expression) collect {
      case e: AggregateFunction => e
    }

    val eliminateDistinctAggs = (_: Expression) transformDown {
      case e: DistinctAggregateFunction => AggregationAlias(e).attr
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
    case e: WindowFunction => WindowAlias(e).attr
  }
}
