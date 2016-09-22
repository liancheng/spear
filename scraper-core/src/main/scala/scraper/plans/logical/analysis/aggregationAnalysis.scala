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
 * A [[Filter]] directly over an [[UnresolvedAggregate]] corresponds to a `HAVING` clause. Its
 * predicate must be resolved together with the [[UnresolvedAggregate]] operator. This rule absorbs
 * such [[Filter]] operators into the [[UnresolvedAggregate]] operators beneath them.
 *
 * @see [[ResolveAggregates]]
 */
class AbsorbHavingConditionsIntoAggregates(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case (agg: UnresolvedAggregate) Filter condition if agg.projectList forall (_.isResolved) =>
      // Tries to resolve and unalias all unresolved attributes using project list output.
      val rewrittenCondition = resolveAndUnaliasUsing(agg.projectList)(condition)

      // `HAVING` predicates are always evaluated before window functions, therefore `HAVING`
      // predicates must not reference window functions or aliases of window functions.
      rewrittenCondition transformUp {
        case _: WindowFunction | _: WindowAttribute =>
          throw new IllegalAggregationException(
            "HAVING clauses are not allowed to reference any window functions or their aliases."
          )
      }

      // All having conditions should be preserved.
      agg.copy(havingConditions = agg.havingConditions :+ rewrittenCondition)
  }
}

/**
 * A [[Sort]] directly over an [[UnresolvedAggregate]] is special, its sort ordering expressions
 * must be resolved together with the [[UnresolvedAggregate]] operator. This rule absorbs such
 * [[Sort]] operators into the [[UnresolvedAggregate]] operators beneath them.
 *
 * @see [[ResolveAggregates]]
 */
class AbsorbSortsIntoAggregates(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case (agg: UnresolvedAggregate) Sort order if agg.projectList forall (_.isResolved) =>
      // Tries to resolve and unalias all unresolved attributes using project list output.
      val rewrittenOrder = order map resolveAndUnaliasUsing(agg.projectList)

      // Only preserves the last sort order.
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
 * This rule resolves an [[UnresolvedAggregate]] into a combination of the following operators:
 *
 *  - an [[Aggregate]], which performs the aggregation, and
 *  - an optional [[Filter]], which corresponds to the `HAVING` clause, and
 *  - zero or more [[Window]]s, which are responsible for evaluating window functions, and
 *  - an optional [[Sort]], which corresponds to the `ORDER BY` clause, and
 *  - a top-level [[Project]], used to assemble the final output attributes.
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
class ResolveAggregates(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan =
    // Only executes this rule when all the pre-conditions hold.
    tree collectFirst preConditionViolations map (_ => tree) getOrElse {
      tree transformDown resolveUnresolvedAggregate
    }

  // This partial function performs as a guard, who ensures all the pre-conditions of this analysis
  // rule. We should skip this rule by returning the original query plan whenever the plan tree
  // contains any of the following patterns.
  private val preConditionViolations: PartialFunction[LogicalPlan, Unit] = {
    // Waits until all adjacent having conditions are absorbed.
    case ((_: UnresolvedAggregate) Filter _) => ()

    // Waits until all adjacent sorts are absorbed.
    case ((_: UnresolvedAggregate) Sort _) => ()

    // Waits until project list, having condition, and sort order expressions are all resolved.
    case plan: UnresolvedAggregate if plan.expressions exists (!_.isResolved) => ()

    // Waits until all distinct aggregate functions are rewritten into normal aggregate functions.
    case plan: UnresolvedAggregate if hasDistinctAggregateFunction(plan.projectList) => ()
  }

  // This partial function is the one who does all the real heavy work. It's one of the most complex
  // and subtlest piece of code throughout the whole project... Aggregation, what a beast...
  private val resolveUnresolvedAggregate: PartialFunction[LogicalPlan, LogicalPlan] = {
    case agg @ UnresolvedAggregate(Resolved(child), keys, projectList, conditions, order) =>
      // Aliases all grouping keys and builds a grouping key rewriter map.
      val keyAliases = keys map (GroupingAlias(_))
      val keyRewriter = buildRewriter(keyAliases)
      val keyRestorer = buildRestorer(keyAliases)

      // A function that rewrites all grouping keys in a given expression to their corresponding
      // `GroupingAttribute`s except for grouping keys appearing as (part of) arguments of non-
      // window aggregate functions.
      val rewriteKeys = (_: Expression) transformUp keyRewriter transformUp {
        case e: AggregateFunction => e transformUp keyRestorer
        case e: WindowFunction    => e.copy(function = e.function transformUp keyRewriter)
      }

      // Collects and aliases all aggregate functions and builds an aggregate function rewriter map.
      val aggs = collectAggregateFunctions(projectList ++ conditions ++ order map rewriteKeys)
      val aggAliases = aggs map (AggregationAlias(_))
      val aggRewriter = buildRewriter(aggAliases)
      val aggRestorer = buildRestorer(aggAliases)

      // Checks for invalid nested aggregate functions like `MAX(COUNT(*))`.
      aggs foreach rejectNestedAggregateFunction

      val rewriteAggs = (_: Expression) transformUp aggRewriter transformUp {
        case e: WindowFunction =>
          e.copy(function = e.function transformUp aggRestorer)
      }

      // Collects and aliases all window functions. Note that grouping keys referenced by the window
      // functions and their window specs must be rewritten to `GroupingAttribute`s first before
      // aliasing. This is because window functions are evaluated after aggregation, thus grouping
      // keys can only be referenced by window functions in the form of `GroupingAttribute`s.
      val wins = collectWindowFunctions(projectList ++ order map (rewriteKeys andThen rewriteAggs))
      val winAliases = wins map (WindowAlias(_))
      val winRewriter = buildRewriter(winAliases)
      val winRestorer = buildRestorer(winAliases)

      val rewriteWins = (_: Expression) transformUp winRewriter

      // A function that rewrites aggregate functions, grouping keys, and window functions to
      // corresponding `InternalAttribute`s. The order of transformations is significant.
      val rewrite = rewriteKeys andThen rewriteAggs andThen rewriteWins

      // A function used to restore `InternalAttribute`s to the original expressions for error
      // reporting purposes. Same as above, the order of transformations is significant.
      val restore = (_: Expression)
        .transformUp(winRestorer)
        .transformUp(aggRestorer)
        .transformUp(keyRestorer)

      // While being used in aggregations, the only input expressions a window function (and its
      // window specs) can reference are the grouping keys. E.g., these queries are valid:
      //
      //   SELECT max(a) OVER (PARTITION BY a % 10) FROM t GROUP BY a
      //              ~                     ~                       ~
      //   SELECT sum(a % 10 + 1) OVER (ORDER BY a % 10 DESC) FROM t GROUP BY a % 10
      //              ~~~~~~                     ~~~~~~                       ~~~~~~
      //
      // while these are not:
      //
      //   -- `b` is not a grouping key
      //   SELECT max(b) OVER (...) FROM t GROUP BY a
      //
      //   -- `a` is not a grouping key
      //   SELECT sum(a) OVER (...) FROM t GROUP BY a % 10
      //
      // Therefore, after rewriting all grouping keys to their corresponding `GroupingAttribute`s,
      // no other input attributes should be found in the rewritten window functions anymore.
      rejectDanglingAttributes("window function", wins map (_.function), keys, Nil, restore)

      // Rewrites grouping keys, aggregate functions, and window functions appearing in having
      // condition, sort ordering expressions, and projected named expressions to corresponding
      // `GroupingAttribute`s, `AggregationAttribute`s, and `WindowAttribute`s.
      val rewrittenConditions = conditions map rewrite
      val rewrittenOrder = order map (ord => ord.copy(child = rewrite(ord.child)))
      val rewrittenProjectList = projectList map rewrite zip projectList map {
        // Top level `InternalAttribute`s must be aliased back to names and expression IDs of the
        // original named expressions so that the transformed plan still has the same output
        // attributes as the original plan.
        case (g: InternalAttribute, e) => g as e.name withID e.expressionID
        case (e: NamedExpression, _)   => e
      }

      // Expressions appearing in an aggregation must conform to the following constraints:
      //
      //  1. An input attribute appearing in a projected named expression must appear as either
      //
      //     * part of a grouping key, or
      //     * (part of) an argument of a non-window aggregate function.
      //
      //     E.g., these queries are valid:
      //
      //       -- `a` appears as part of grouping key `a + 1`.
      //       SELECT a + 1 FROM t GROUP BY a + 1
      //       SELECT a + 1 + 1 FROM t GROUP BY a + 1
      //       SELECT a + 1 + (a + 1) FROM t GROUP BY a + 1
      //
      //       -- `b` appears as the argument of non-window aggregate function `max`.
      //       SELECT max(b) FROM t GROUP BY a + 1
      //
      //       -- `b` appears as part of the argument of non-window aggregate function `avg`.
      //       SELECT avg(b % 10) FROM t GROUP BY a + 1
      //
      //     while these are not:
      //
      //       -- `a + 2` is not a grouping key.
      //       SELECT a + 2 FROM t GROUP BY a + 1
      //
      //       -- The 2nd `a` is not part of the grouping key `a + 1`. Note that the fully
      //       -- parenthesized form of `a + 1 + a + 1` is `(((a + 1) + a) + 1)`.
      //       SELECT a + 1 + a + 1 FROM t GROUP BY a + 1
      //
      //       -- `b` appears as the argument of window aggregate function `max(b) OVER (...)`.
      //       SELECT max(b) OVER (...) FROM t GROUP BY a + 1
      //
      //  2. An input attribute appearing in HAVING and ORDER BY clauses must appear as
      //
      //     * part of a grouping key, or
      //     * (part of) an argument of a non-window aggregate function, or
      //     * an output attribute of the project list.
      //
      //     E.g., these queries are valid:
      //
      //       SELECT max(b) AS max FROM t GROUP BY a + 1 ORDER BY a + 1 DESC
      //                                            ~~~~~          ~~~~~
      //       SELECT max(b) AS max FROM t GROUP BY a + 1 HAVING sum(a) < 10
      //                                                         ~~~~~~
      //       SELECT max(b) AS max FROM t GROUP BY a + 1 HAVING max > 0
      //              ~~~~~~~~~~~~~                              ~~~
      //
      //     while these are not:
      //
      //       -- `a` is not a projected output attribute and `a + 2` is not a grouping key.
      //       SELECT max(b) AS max FROM t GROUP BY a + 1 HAVING a + 2 < 10
      //
      //       -- `b` is neither a projected output attribute nor a grouping key.
      //       SELECT max(b) AS max FROM t GROUP BY a + 1 ORDER BY b DESC
      //
      // At this stage, we've already rewritten all grouping keys and aggregate functions to
      // corresponding `GroupingAttribute`s and `AggregationAttribute`s. Therefore, no other input
      // attributes should be found in these rewritten expressions anymore.
      val output = rewrittenProjectList map (_.toAttribute)
      rejectDanglingAttributes("SELECT field", rewrittenProjectList, keys, Nil, restore)
      rejectDanglingAttributes("HAVING condition", rewrittenConditions, keys, output, restore)
      rejectDanglingAttributes("ORDER BY expression", rewrittenOrder, keys, output, restore)

      child
        // The main aggregation.
        .resolvedGroupBy(keyAliases)
        .agg(aggAliases)
        // `HAVING` clause.
        .filterOption(rewrittenConditions)
        // Stacks one `Window` operator for each window spec.
        .windowsOption(winAliases)
        // `ORDER BY` clause.
        .orderByOption(rewrittenOrder)
        // Evaluates all non-window and non-aggregate expressions and cleans up output attributes.
        .select(rewrittenProjectList)
  }

  private def collectAggregateFunctions(expressions: Seq[Expression]): Seq[AggregateFunction] = {
    val removeWindowAggs = (_: Expression) transformDown {
      case e @ WindowFunction(f: AggregateFunction, _) =>
        e.copy(function = AggregationAlias(f).toAttribute)
    }

    val removeDistinctAggs = (_: Expression) transformDown {
      case e: DistinctAggregateFunction => AggregationAlias(e).toAttribute
    }

    val collectDistinctAggs = removeWindowAggs andThen {
      _ collect { case e: DistinctAggregateFunction => e }
    }

    val collectAggs = removeWindowAggs andThen removeDistinctAggs andThen {
      _ collect { case e: AggregateFunction => e }
    }

    (expressions.flatMap(collectDistinctAggs) ++ expressions.flatMap(collectAggs)).distinct
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
    restore: Expression => Expression
  ): Unit = expressions foreach { e =>
    e.references collectFirst {
      case a: AttributeRef if !(outputAttributes contains a) =>
        throw new IllegalAggregationException(kind, a, restore(e), groupingKeys)
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
  def hasAggregateFunction(expressions: Seq[Expression]): Boolean =
    expressions exists hasAggregateFunction

  def hasAggregateFunction(expression: Expression): Boolean =
    expression.transformDown {
      // Excludes aggregate functions in window functions
      case e: WindowFunction => WindowAlias(e).toAttribute
    }.collectFirst {
      case e: AggregateFunction => ()
    }.nonEmpty

  def resolveAndUnaliasUsing[E <: Expression](input: Seq[NamedExpression]): E => E =
    Expression.resolveUsing[E](input) _ andThen Alias.unaliasUsing[E](input)
}
