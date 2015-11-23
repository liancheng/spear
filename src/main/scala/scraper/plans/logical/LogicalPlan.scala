package scraper.plans.logical

import scala.reflect.runtime.universe.WeakTypeTag
import scala.util.Try

import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.QueryPlan
import scraper.reflection.schemaOf
import scraper.types.TupleType
import scraper.{LogicalPlanUnresolved, Row}

trait LogicalPlan extends QueryPlan[LogicalPlan] {
  def resolved: Boolean = expressions.forall(_.resolved) && children.forall(_.resolved)

  def strictlyTyped: Try[LogicalPlan] = Try {
    this transformExpressionsDown {
      case e => e.strictlyTyped.get
    }
  }

  def select(projections: Seq[NamedExpression]): LogicalPlan = Project(this, projections)

  def select(first: NamedExpression, rest: NamedExpression*): LogicalPlan = select(first +: rest)

  def filter(condition: Predicate): LogicalPlan = Filter(this, condition)

  def limit(n: Expression): LogicalPlan = Limit(this, n)

  def limit(n: Int): LogicalPlan = this limit lit(n)
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

  override def nodeCaption: String =
    s"${getClass.getSimpleName} ${output map (_.nodeCaption) mkString ", "}"
}

object LocalRelation {
  def apply[T <: Product: WeakTypeTag](data: Traversable[T]): LocalRelation = {
    val schema = schemaOf[T].dataType match { case t: TupleType => t }
    val rows = data.map { product => new Row(product.productIterator.toSeq) }
    LocalRelation(rows, schema)
  }
}

case class Project(child: LogicalPlan, projections: Seq[NamedExpression])
  extends UnaryLogicalPlan {

  override def expressions: Seq[Expression] = projections

  override lazy val output: Seq[Attribute] = projections map (_.toAttribute)

  override def nodeCaption: String =
    s"${getClass.getSimpleName} ${projections map (_.nodeCaption) mkString ", "}"
}

case class Filter(child: LogicalPlan, condition: Predicate) extends UnaryLogicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  override def nodeCaption: String =
    s"${getClass.getSimpleName} ${condition.nodeCaption}"
}

case class Limit(child: LogicalPlan, limit: Expression) extends UnaryLogicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  override lazy val strictlyTyped: Try[LogicalPlan] = for {
    n <- limit.strictlyTyped if n.foldable
  } yield if (n sameOrEqual limit) this else copy(limit = n)

  override def nodeCaption: String =
    s"${getClass.getSimpleName} ${limit.nodeCaption}"
}
