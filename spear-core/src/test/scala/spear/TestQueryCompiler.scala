package spear

import org.mockito.Mockito._

import spear.plans.logical.LogicalPlan
import spear.plans.physical.PhysicalPlan

class TestQueryCompiler extends BasicQueryCompiler {
  override def plan(plan: LogicalPlan): PhysicalPlan = {
    val physicalPlan = mock(classOf[PhysicalPlan])
    when(physicalPlan.iterator).thenReturn(Iterator.empty).getMock[PhysicalPlan]
  }
}
