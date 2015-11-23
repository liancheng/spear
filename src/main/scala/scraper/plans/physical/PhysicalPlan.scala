package scraper.plans.physical

import scraper.Row
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

case object SingleRowRelation extends LeafPhysicalPlan {
  override def iterator: Iterator[Row] = Iterator.single(Row.empty)

  override val output: Seq[Attribute] = Nil
}

case class LocalRelation(data: Iterator[Row], override val output: Seq[Attribute])
  extends LeafPhysicalPlan {

  override def iterator: Iterator[Row] = data

  override def nodeCaption: String =
    s"${getClass.getSimpleName} ${output.map(_.nodeCaption).mkString(", ")}"
}

case class Project(child: PhysicalPlan, override val expressions: Seq[NamedExpression])
  extends UnaryPhysicalPlan {

  override val output: Seq[Attribute] = expressions.map(_.toAttribute)

  override def iterator: Iterator[Row] = child.iterator.map { row =>
    val boundProjections = expressions.map(BoundRef.bind(_, child.output))
    new Row(boundProjections map (_ evaluate row))
  }

  override def nodeCaption: String =
    s"${getClass.getSimpleName} ${expressions.map(_.nodeCaption).mkString(", ")}"
}

case class Filter(child: PhysicalPlan, condition: Predicate) extends UnaryPhysicalPlan {
  override val output: Seq[Attribute] = child.output

  override def iterator: Iterator[Row] = {
    val boundCondition = BoundRef.bind(condition, child.output)
    child.iterator.filter { row =>
      boundCondition.evaluate(row).asInstanceOf[Boolean]
    }
  }

  override def nodeCaption: String =
    s"${getClass.getSimpleName} ${condition.nodeCaption}"
}

case class Limit(child: PhysicalPlan, limit: Expression) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  override def iterator: Iterator[Row] = child.iterator take limit.evaluated.asInstanceOf[Int]

  override def nodeCaption: String =
    s"${getClass.getSimpleName} ${limit.nodeCaption}"
}
