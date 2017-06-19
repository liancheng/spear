package spear.plans.physical

import spear._
import spear.expressions._
import spear.plans.QueryPlan
import spear.plans.logical.annotations.Explain

trait PhysicalPlan extends QueryPlan[PhysicalPlan] {
  def iterator: Iterator[Row]

  def requireMaterialization: Boolean = children exists { _.requireMaterialization }
}

trait LeafPhysicalPlan extends PhysicalPlan {
  override def children: Seq[PhysicalPlan] = Nil
}

trait UnaryPhysicalPlan extends PhysicalPlan {
  def child: PhysicalPlan

  override def children: Seq[PhysicalPlan] = Seq(child)
}

trait BinaryPhysicalPlan extends PhysicalPlan {
  def left: PhysicalPlan

  def right: PhysicalPlan

  override def children: Seq[PhysicalPlan] = Seq(left, right)
}

case object SingleRowRelation extends LeafPhysicalPlan {
  override def iterator: Iterator[Row] = Iterator single Row.empty

  override val output: Seq[Attribute] = Nil
}

case class NotImplemented(
  logicalPlanName: String,
  @Explain(hidden = true) input: Seq[PhysicalPlan],
  output: Seq[Attribute]
) extends PhysicalPlan {
  override def children: Seq[PhysicalPlan] = input

  override def iterator: Iterator[Row] = throw new UnsupportedOperationException(
    s"$logicalPlanName is not implemented yet"
  )
}
