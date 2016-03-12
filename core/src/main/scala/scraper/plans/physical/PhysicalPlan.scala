package scraper.plans.physical

import scraper._
import scraper.expressions._
import scraper.plans.QueryPlan

trait PhysicalPlan extends QueryPlan[PhysicalPlan] {
  def iterator: Iterator[Row]
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

case object EmptyRelation extends LeafPhysicalPlan {
  override def iterator: Iterator[Row] = Iterator.empty

  override val output: Seq[Attribute] = Nil
}

case object SingleRowRelation extends LeafPhysicalPlan {
  override def iterator: Iterator[Row] = Iterator single Row.empty

  override val output: Seq[Attribute] = Nil
}

case class NotImplemented(
  input: Seq[PhysicalPlan], output: Seq[Attribute]
)(logicalPlanNodeName: String)
  extends PhysicalPlan {

  override def nodeName: String = s"*$logicalPlanNodeName NOT IMPLEMENTED*"

  override def children: Seq[PhysicalPlan] = input

  override def iterator: Iterator[Row] = throw new UnsupportedOperationException(
    s"$logicalPlanNodeName is not implemented yet"
  )
}
