package scraper.plans.logical

import scala.reflect.runtime.universe.WeakTypeTag

import scraper.expressions.{ Predicate, Attribute, Expression, NamedExpression }
import scraper.plans.QueryPlan
import scraper.reflection.schemaOf
import scraper.types.TupleType
import scraper.{ LogicalPlanUnresolved, Row }

trait LogicalPlan extends QueryPlan[LogicalPlan] {
  def resolved: Boolean = expressions.forall(_.resolved) && children.forall(_.resolved)
}

trait UnresolvedLogicalPlan extends LogicalPlan {
  override def output: Seq[Attribute] = throw LogicalPlanUnresolved(this)

  override def resolved: Boolean = false
}

trait LeafLogicalPlan extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq.empty
}

trait UnaryLogicalPlan extends LogicalPlan {
  def child: LogicalPlan

  override def children: Seq[LogicalPlan] = Seq(child)
}

case class UnresolvedRelation(name: String) extends LeafLogicalPlan with UnresolvedLogicalPlan

case object SingleRowRelation extends LeafLogicalPlan {
  override val output: Seq[Attribute] = Nil
}

case class LocalRelation(data: Traversable[Row], schema: TupleType)
  extends LeafLogicalPlan {

  override val output: Seq[Attribute] = schema.toAttributes

  override def caption: String =
    s"${getClass.getSimpleName} ${output.map(_.caption).mkString(", ")}"
}

object LocalRelation {
  def apply[T <: Product: WeakTypeTag](data: Traversable[T]): LocalRelation = {
    val schema = schemaOf[T].dataType match { case t: TupleType => t }
    val rows = data.map { product => new Row(product.productIterator.toSeq) }
    LocalRelation(rows, schema)
  }
}

case class Project(override val expressions: Seq[NamedExpression], child: LogicalPlan)
  extends UnaryLogicalPlan {

  override val output: Seq[Attribute] = expressions.map(_.toAttribute)

  override def caption: String =
    s"${getClass.getSimpleName} ${expressions map (_.caption) mkString ", "}"
}

case class Filter(condition: Predicate, child: LogicalPlan) extends UnaryLogicalPlan {
  override val output: Seq[Attribute] = child.output

  override def caption: String = s"${getClass.getSimpleName} ${condition.caption}"
}
