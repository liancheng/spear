package spear.local

import spear._
import spear.config.Settings
import spear.local.plans.physical
import spear.local.plans.physical.HashAggregate
import spear.local.plans.physical.dsl._
import spear.plans.QueryPlanner
import spear.plans.logical._
import spear.plans.physical.{NotImplemented, PhysicalPlan}

class LocalQueryCompiler extends BasicQueryCompiler {
  override def plan(plan: LogicalPlan): PhysicalPlan = planner apply plan

  private val planner = new LocalQueryPlanner
}

class LocalQueryPlanner extends QueryPlanner[LogicalPlan, PhysicalPlan] {
  override def strategies: Seq[Strategy] = Seq(BasicOperators)

  object BasicOperators extends Strategy {
    override def apply(logicalPlan: LogicalPlan): Seq[PhysicalPlan] = logicalPlan match {
      case relation @ LocalRelation(data, _) =>
        physical.LocalRelation(data, relation.output) :: Nil

      case child Project projectList =>
        (planLater(child) select projectList) :: Nil

      case Aggregate(child, keys, functions) =>
        HashAggregate(planLater(child), keys, functions) :: Nil

      case child Filter condition =>
        (planLater(child) filter condition) :: Nil

      case child Limit n =>
        (planLater(child) limit n) :: Nil

      case Join(left, right, Inner, Some(condition)) =>
        (planLater(left) cartesianJoin planLater(right) on condition) :: Nil

      case Join(left, right, Inner, _) =>
        (planLater(left) cartesianJoin planLater(right)) :: Nil

      case child Sort order =>
        (planLater(child) orderBy order) :: Nil

      case child Subquery _ =>
        planLater(child) :: Nil

      case _: SingleRowRelation =>
        spear.plans.physical.SingleRowRelation :: Nil

      case left Union right =>
        (planLater(left) union planLater(right)) :: Nil

      case left Intersect right =>
        (planLater(left) intersect planLater(right)) :: Nil

      case left Except right =>
        (planLater(left) except planLater(right)) :: Nil

      case plan =>
        NotImplemented(plan.nodeName.toString, plan.children map planLater, plan.output) :: Nil
    }
  }
}
