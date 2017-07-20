package spear

import org.mockito.Mockito._

import spear.plans.logical.LogicalPlan
import spear.plans.physical.PhysicalPlan

class TestQueryCompiler extends BasicQueryCompiler {
  override def plan(plan: LogicalPlan): PhysicalPlan = when {
    mock(classOf[PhysicalPlan]).iterator
  }.thenReturn {
    Iterator.empty
  }.getMock[PhysicalPlan]
}
