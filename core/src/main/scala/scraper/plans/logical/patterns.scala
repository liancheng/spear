package scraper.plans.logical

import scraper.expressions
import scraper.expressions.Expression

/** A simple pattern that matches unresolved [[LogicalPlan]]s and [[expressions.Expression]]s */
object Unresolved {
  def unapply(plan: LogicalPlan): Option[LogicalPlan] = Some(plan) filter (!_.isResolved)

  def unapply(expression: Expression): Option[Expression] =
    Some(expression) filter (!_.isResolved)
}

/** A simple pattern that matches resolved [[LogicalPlan]]s and [[expressions.Expression]]s */
object Resolved {
  def unapply(plan: LogicalPlan): Option[LogicalPlan] = Some(plan) filter (_.isResolved)

  def unapply(expression: Expression): Option[Expression] = Some(expression) filter (_.isResolved)
}
