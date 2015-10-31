package scraper.plans.execution

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

case class LocalRelation(data: Iterator[Row], override val output: Seq[Attribute])
  extends LeafPhysicalPlan {

  override def iterator: Iterator[Row] = data
}

case class Project(override val expressions: Seq[NamedExpression], child: PhysicalPlan)
  extends UnaryPhysicalPlan {

  override def output: Seq[Attribute] = expressions.map(_.toAttribute)

  override def iterator: Iterator[Row] = child.iterator.map { row =>
    new Row(expressions map (_ evaluate row))
  }
}

case class Filter(condition: Predicate, child: PhysicalPlan) extends UnaryPhysicalPlan {
  override def output: Seq[Attribute] = child.output

  override def iterator: Iterator[Row] = child.iterator.filter { row =>
    condition.evaluate(row).asInstanceOf[Boolean]
  }
}
