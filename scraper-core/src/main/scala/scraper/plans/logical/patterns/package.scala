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

  object GenericProject {
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
      case p @ Project(_, WindowSeq(windowFunctions, a: Aggregate)) =>
        val restore = restoreInternalAttributes(a.keys, a.functions, windowFunctions)
        Some((p.projectList map restore, Nil, None, a.keys map { _.child }, a.child))

      case p @ Project(_, WindowSeq(windowFunctions, f @ Filter(_, a: Aggregate))) =>
        val restore = restoreInternalAttributes(a.keys, a.functions, windowFunctions)
        Some((
          p.projectList map restore,
          Nil,
          Some(restore(f.condition)),
          a.keys map { _.child },
          a.child
        ))

      case p @ Project(_, WindowSeq(windowFunctions, s @ Sort(_, a: Aggregate))) =>
        val restore = restoreInternalAttributes(a.keys, a.functions, windowFunctions)
        Some((
          p.projectList map restore,
          s.order map restore,
          None,
          a.keys map { _.child },
          a.child
        ))

      case p @ Project(_, f @ Filter(_, WindowSeq(windowFunctions, s @ Sort(_, a: Aggregate)))) =>
        val restore = restoreInternalAttributes(a.keys, a.functions, windowFunctions)
        Some((
          p.projectList map restore,
          s.order map restore,
          Some(restore(f.condition)),
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
    def unapply(tree: LogicalPlan): Option[(Seq[WindowAlias], LogicalPlan)] = tree match {
      case plan @ Window(_, _, _, WindowSeq(functions, child)) =>
        Some((plan.functions ++ functions, child))

      case plan =>
        Some((Nil, plan))
    }
  }
}
