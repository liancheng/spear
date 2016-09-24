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
        case _: WindowFunction =>
          throw new IllegalAggregationException(
            "Window functions are not allowed in HAVING clauses."
          )

        case _: WindowAttribute =>
          throw new IllegalAggregationException(
            "Window function aliases are not allowed to be referenced in HAVING clauses."
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
    case ((_: UnresolvedAggregate) Filter _) =>

    // Waits until all adjacent sorts are absorbed.
    case ((_: UnresolvedAggregate) Sort _) =>

    // Waits until project list, having condition, and sort order expressions are all resolved.
    case plan: UnresolvedAggregate if plan.expressions exists (!_.isResolved) =>

    // Waits until all distinct aggregate functions are rewritten into normal aggregate functions.
    case plan: UnresolvedAggregate if hasDistinctAggregateFunction(plan.projectList) =>
  }

  private val resolveUnresolvedAggregate: PartialFunction[LogicalPlan, LogicalPlan] = {
    case agg @ UnresolvedAggregate(Resolved(child), keys, projectList, conditions, order) =>
      val keyAliases = keys map (GroupingAlias(_))
      val rewriteKeys = (_: Expression) transformUp buildRewriter(keyAliases)
      val restoreKeys = (_: Expression) transformUp buildRestorer(keyAliases)

      val aggs = collectAggregateFunctions(projectList ++ conditions ++ order)
      aggs foreach rejectNestedAggregateFunction

      val aggAliases = aggs map (AggregationAlias(_))
      val restoreAggs = (_: Expression) transformUp buildRestorer(aggAliases)
      val rewriteAggs = (_: Expression) transformUp buildRewriter(aggAliases) transformUp {
        // Window aggregate functions should not be rewritten. Recovers them here. E.g.:
        //
        //   SELECT max(a) OVER (PARTITION BY max(a)) FROM t GROUP BY a
        //
        // The 2nd `max(a)` should be rewritten while the 1st one must be preserved.
        case e @ WindowFunction(f, _) => e.copy(function = restoreAggs(f))
      }

      // Note: window functions may appear in both SELECT and ORDER BY clauses.
      val wins = collectWindowFunctions(projectList ++ order map (rewriteAggs andThen rewriteKeys))
      val winAliases = wins map (WindowAlias(_))
      val rewriteWins = (_: Expression) transformUp buildRewriter(winAliases)
      val restoreWins = (_: Expression) transformUp buildRestorer(winAliases)

      val rewrite = rewriteAggs andThen rewriteKeys andThen rewriteWins
      val restore = restoreWins andThen restoreKeys andThen restoreAggs

      // `InternalAttribute`s should not be exposed outside. Aliases them using names and expression
      // IDs of the original named expressions.
      def rewriteNamedExpression(named: NamedExpression): NamedExpression = rewrite(named) match {
        case e: InternalAttribute => e as named.name withID named.expressionID
        case e: NamedExpression   => e
      }

      val rewrittenProjectList = projectList map rewriteNamedExpression
      val rewrittenConditions = conditions map rewrite
      val rewrittenOrder = order map rewrite

      def rejectDanglingAttributes(kind: String, whitelist: Seq[Attribute] = Nil)(e: Expression) =
        e.references collectFirst {
          case a: AttributeRef if !(whitelist contains a) =>
            throw new IllegalAggregationException(
              s"""Attribute ${a.sqlLike} in $kind ${restore(e).sqlLike} is neither referenced by any
                 |non-window aggregate functions nor a grouping key among
                 |${keys map (_.sqlLike) mkString ("[", ", ", "]")}.
                 |""".oneLine
            )
        }

      val output = rewrittenProjectList map (_.attr)
      wins map (_.function) foreach rejectDanglingAttributes("window function")
      rewrittenProjectList foreach rejectDanglingAttributes("SELECT field")
      rewrittenConditions foreach rejectDanglingAttributes("HAVING condition", output)
      rewrittenOrder foreach rejectDanglingAttributes("ORDER BY expression", output)

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
            s"""Aggregate function can't be nested within another aggregate function:
               |${e.sqlLike}
               |""".oneLine
          )
      })
  }

  private def hasDistinctAggregateFunction(expressions: Seq[Expression]): Boolean =
    expressions exists hasDistinctAggregateFunction

  private def hasDistinctAggregateFunction(expression: Expression): Boolean =
    eliminateWindowFunctions(expression).collectFirst {
      case e: DistinctAggregateFunction =>
    }.nonEmpty
}

object AggregationAnalysis {
  def hasAggregateFunction(expressions: Seq[Expression]): Boolean =
    collectAggregateFunctions(expressions).nonEmpty

  def resolveAndUnaliasUsing[E <: Expression](input: Seq[NamedExpression]): E => E =
    Expression.resolveUsing[E](input) _ andThen Alias.unaliasUsing[E](input)

  def collectAggregateFunctions(expressions: Seq[Expression]): Seq[AggregateFunction] =
    expressions.flatMap(collectAggregateFunctions).distinct

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

    val aggsInsideWindowFunctions = for {
      nested <- expression.collect { case WindowFunction(f, spec) => f.children :+ spec }
      agg <- collectAggregateFunctions(nested)
    } yield agg

    val windowFunctionsEliminated = eliminateWindowFunctions(expression)

    val aggsOutsideWindowFunctions = for {
      collect <- Seq(collectDistinctAggs, eliminateDistinctAggs andThen collectAggs)
      agg <- collect(windowFunctionsEliminated)
    } yield agg

    (aggsInsideWindowFunctions ++ aggsOutsideWindowFunctions).distinct
  }

  def eliminateWindowFunctions(expression: Expression): Expression = expression transformDown {
    case e: WindowFunction => WindowAlias(e).attr
  }
}
