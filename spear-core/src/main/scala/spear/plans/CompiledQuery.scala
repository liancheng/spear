package spear.plans

import spear.Context
import spear.plans.logical.LogicalPlan
import spear.plans.physical.PhysicalPlan

class CompiledQuery(val context: Context, val logicalPlan: LogicalPlan) {
  lazy val analyzedPlan: LogicalPlan = context.queryCompiler analyze logicalPlan

  lazy val optimizedPlan: LogicalPlan = context.queryCompiler optimize analyzedPlan

  lazy val physicalPlan: PhysicalPlan = context.queryCompiler plan optimizedPlan
}
