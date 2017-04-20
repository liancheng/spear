package scraper.plans.logical

import scraper.expressions.{AggregationAlias, Expression, GroupingAlias, WindowAlias}
import scraper.expressions.InternalAlias.buildRestorer

package object patterns {
  /** A simple pattern that matches unresolved logical plans and expressions */
  object Unresolved {
    def unapply(plan: LogicalPlan): Option[LogicalPlan] =
      Some(plan) filter { !_.isResolved }

    def unapply(expression: Expression): Option[Expression] =
      Some(expression) filter { !_.isResolved }
  }

  /** A simple pattern that matches resolved logical plans and expressions */
  object Resolved {
    def unapply(plan: LogicalPlan): Option[LogicalPlan] =
      Some(plan) filter { _.isResolved }

    def unapply(expression: Expression): Option[Expression] =
      Some(expression) filter { _.isResolved }
  }

  object ResolvedAggregate {
    // format: OFF
    type ResultType = (
      Seq[Expression],      // Project list
      Seq[Expression],      // Sort order
      Option[Expression],   // HAVING condition
      Seq[Expression],      // Window functions
      LogicalPlan           // Child plan
    )
    // format: ON

    def unapply(tree: LogicalPlan): Option[ResultType] = tree match {
      case (a: Aggregate) WindowSeq windowFunctions Project output =>
        val restore = restoreInternalAttributes(a.keys, a.functions, windowFunctions)
        Some((output map restore, Nil, None, a.keys map { _.child }, a.child))

      case (a: Aggregate) Filter condition WindowSeq windowFunctions Project output =>
        val restore = restoreInternalAttributes(a.keys, a.functions, windowFunctions)
        Some((
          output map restore,
          Nil,
          Some(restore(condition)),
          a.keys map { _.child },
          a.child
        ))

      case (a: Aggregate) WindowSeq windowFunctions Sort order Project output =>
        val restore = restoreInternalAttributes(a.keys, a.functions, windowFunctions)
        Some((
          output map restore,
          order map restore,
          None,
          a.keys map { _.child },
          a.child
        ))

      case (a: Aggregate) Filter condition WindowSeq windowFunctions Sort order Project output =>
        val restore = restoreInternalAttributes(a.keys, a.functions, windowFunctions)
        Some((
          output map restore,
          order map restore,
          Some(restore(condition)),
          a.keys map { _.child },
          a.child
        ))
    }

    def restoreInternalAttributes(
      keys: Seq[GroupingAlias],
      aggs: Seq[AggregationAlias],
      wins: Seq[WindowAlias]
    ): Expression => Expression = {
      val restoreKeys = (_: Expression) transformUp buildRestorer(keys)
      val restoreAggs = (_: Expression) transformUp buildRestorer(aggs)
      val restoreWins = (_: Expression) transformUp buildRestorer(wins)
      restoreWins andThen restoreAggs andThen restoreKeys
    }
  }

  object WindowSeq {
    def unapply(tree: LogicalPlan): Option[(LogicalPlan, Seq[WindowAlias])] = tree match {
      case plan @ Window(_, _, _, WindowSeq(child, functions)) =>
        Some((child, plan.functions ++ functions))

      case plan =>
        Some((plan, Nil))
    }
  }
}
