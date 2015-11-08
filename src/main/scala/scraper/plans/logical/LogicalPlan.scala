package scraper.plans.logical

import scala.reflect.runtime.universe.WeakTypeTag

import scraper.expressions.{ Attribute, Expression, NamedExpression }
import scraper.plans.QueryPlan
import scraper.reflection.schemaOf
import scraper.types.StructType
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
  override def output: Seq[Attribute] = Nil
}

case class LocalRelation(data: Iterator[Row], schema: StructType) extends LeafLogicalPlan {
  override def output: Seq[Attribute] = schema.toAttributes
}

object LocalRelation {
  def apply[T <: Product: WeakTypeTag](data: Iterable[T]): LocalRelation = {
    val schema = schemaOf[T].dataType match { case t: StructType => t }
    val rows = data.iterator.map { product => new Row(product.productIterator.toSeq) }
    LocalRelation(rows, schema)
  }
}

case class Project(override val expressions: Seq[NamedExpression], child: LogicalPlan)
  extends UnaryLogicalPlan {
  override def output: Seq[Attribute] = expressions.map(_.toAttribute)
}

case class Filter(predicate: Expression, child: LogicalPlan) extends UnaryLogicalPlan {
  override def output: Seq[Attribute] = child.output
}
