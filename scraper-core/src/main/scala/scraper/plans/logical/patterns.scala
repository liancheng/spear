package scraper.plans.logical

import scraper.expressions.Expression

/** A simple pattern that matches unresolved logical plans and expressions */
object Unresolved {
  def unapply(plan: LogicalPlan): Option[LogicalPlan] =
    Some(plan) filter { !_.isResolved }

  def unapply(expression: Expression): Option[Expression] =
    Some(expression) filter { !_.isResolved }
}

/** A simple pattern that matches resolved logical plans and expressions */
object Resolved {
  def unapply(plan: LogicalPlan): Option[LogicalPlan] = Some(plan) filter { _.isResolved }

  def unapply(expression: Expression): Option[Expression] = Some(expression) filter { _.isResolved }
}
