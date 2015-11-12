package scraper.plans.physical

import scraper.Row
import scraper.expressions.{ Attribute, NamedExpression, Predicate }
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
  override def iterator: Iterator[Row] = Iterator.single(Row())

  override def output: Seq[Attribute] = Nil
}

case class LocalRelation(data: Iterator[Row], override val output: Seq[Attribute])
  extends LeafPhysicalPlan {

  override def iterator: Iterator[Row] = data

  override def nodeDescription: String =
    s"${getClass.getSimpleName} ${output.map(_.nodeDescription).mkString(", ")}"
}

case class Project(override val expressions: Seq[NamedExpression], child: PhysicalPlan)
  extends UnaryPhysicalPlan {

  override def output: Seq[Attribute] = expressions.map(_.toAttribute)

  override def iterator: Iterator[Row] = child.iterator.map { row =>
    new Row(expressions map (_ evaluate row))
  }

  override def nodeDescription: String =
    s"${getClass.getSimpleName} ${expressions.map(_.nodeDescription).mkString(", ")}"
}

case class Filter(condition: Predicate, child: PhysicalPlan) extends UnaryPhysicalPlan {
  override def output: Seq[Attribute] = child.output

  override def iterator: Iterator[Row] = child.iterator.filter { row =>
    condition.evaluate(row).asInstanceOf[Boolean]
  }

  override def nodeDescription: String = s"${getClass.getSimpleName} ${condition.nodeDescription}"
}
