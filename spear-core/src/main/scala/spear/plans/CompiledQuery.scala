package spear.plans

import spear.Context
import spear.plans.logical.LogicalPlan
import spear.plans.physical.PhysicalPlan

class CompiledQuery(val context: Context, val logicalPlan: LogicalPlan) {
  lazy val analyzedPlan: LogicalPlan = context.queryExecutor analyze logicalPlan

  lazy val optimizedPlan: LogicalPlan = context.queryExecutor optimize analyzedPlan

  lazy val physicalPlan: PhysicalPlan = context.queryExecutor plan optimizedPlan
}
