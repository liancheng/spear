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

  private val resolveUnresolvedAggregate: PartialFunction[LogicalPlan, LogicalPlan] = {
    case agg @ UnresolvedAggregate(Resolved(child), keys, projectList, conditions, order) =>
      val keyAliases = keys map (GroupingAlias(_))
      val keyRewriter = buildRewriter(keyAliases)
      val keyRestorer = buildRestorer(keyAliases)

      val rewriteKeys = (_: Expression) transformUp keyRewriter transformUp {
        case e: AggregateFunction => e transformUp keyRestorer
        case e: WindowFunction    => e.copy(function = e.function transformUp keyRewriter)
      }

      val aggs = collectAggregateFunctions(projectList ++ conditions ++ order map rewriteKeys)
      val aggAliases = aggs map (AggregationAlias(_))
      val aggRewriter = buildRewriter(aggAliases)
      val aggRestorer = buildRestorer(aggAliases)

      aggs foreach rejectNestedAggregateFunction

      val rewriteAggs = (_: Expression) transformUp aggRewriter transformUp {
        case e: WindowFunction =>
          e.copy(function = e.function transformUp aggRestorer)
      }

      val wins = collectWindowFunctions(projectList ++ order map (rewriteKeys andThen rewriteAggs))
      val winAliases = wins map (WindowAlias(_))
      val winRewriter = buildRewriter(winAliases)
      val winRestorer = buildRestorer(winAliases)

      val rewriteWins = (_: Expression) transformUp winRewriter

      val rewrite = rewriteKeys andThen rewriteAggs andThen rewriteWins

      val restore = (_: Expression)
        .transformUp(winRestorer)
        .transformUp(aggRestorer)
        .transformUp(keyRestorer)

      rejectDanglingAttributes("window function", wins map (_.function), keys, Nil, restore)

      val rewrittenConditions = conditions map rewrite
      val rewrittenOrder = order map rewrite
      val rewrittenProjectList = projectList map rewrite zip projectList map {
        case (g: InternalAttribute, e) => g as e.name withID e.expressionID
        case (e: NamedExpression, _)   => e
      }

      val output = rewrittenProjectList map (_.toAttribute)
      rejectDanglingAttributes("SELECT field", rewrittenProjectList, keys, Nil, restore)
      rejectDanglingAttributes("HAVING condition", rewrittenConditions, keys, output, restore)
      rejectDanglingAttributes("ORDER BY expression", rewrittenOrder, keys, output, restore)

      child
        .resolvedGroupBy(keyAliases)
        .agg(aggAliases)
        .filterOption(rewrittenConditions)
        .windowsOption(winAliases)
        .orderByOption(rewrittenOrder)
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
