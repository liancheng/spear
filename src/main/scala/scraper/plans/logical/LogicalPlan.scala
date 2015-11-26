package scraper.plans.logical

import scala.reflect.runtime.universe.WeakTypeTag
import scala.util.Try

import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.QueryPlan
import scraper.reflection.schemaOf
import scraper.types.TupleType
import scraper.{LogicalPlanUnresolved, Row, TypeCheckException}

trait LogicalPlan extends QueryPlan[LogicalPlan] {
  def resolved: Boolean = expressions.forall(_.resolved) && children.forall(_.resolved)

  def strictlyTyped: Try[LogicalPlan] = Try {
    this transformExpressionsDown {
      case e => e.strictlyTyped.get
    }
  }

  protected def whenStrictlyTyped[T](value: => T): T = (
    strictlyTyped map {
      case e if e sameOrEqual this => value
      case _                       => throw TypeCheckException(this, None)
    }
  ).get

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

trait BinaryLogicalPlan extends LogicalPlan {
  def left: LogicalPlan

  def right: LogicalPlan

  override def children: Seq[LogicalPlan] = Seq(left, right)
}

case class UnresolvedRelation(name: String) extends LeafLogicalPlan with UnresolvedLogicalPlan

case object SingleRowRelation extends LeafLogicalPlan {
  override val output: Seq[Attribute] = Nil
}

case class LocalRelation(data: Traversable[Row], schema: TupleType)
  extends LeafLogicalPlan {

  override val output: Seq[Attribute] = schema.toAttributes

  override def nodeCaption: String =
    s"${getClass.getSimpleName} ${output map (_.annotatedString) mkString ", "}"
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

  assert(projections.nonEmpty, "Project should have at least one expression")

  override def expressions: Seq[Expression] = projections

  override lazy val output: Seq[Attribute] = projections map (_.toAttribute)

  override def nodeCaption: String =
    s"${getClass.getSimpleName} ${projections map (_.annotatedString) mkString ", "}"
}

case class Filter(child: LogicalPlan, condition: Predicate) extends UnaryLogicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  override def nodeCaption: String = s"${getClass.getSimpleName} ${condition.annotatedString}"
}

case class Limit(child: LogicalPlan, limit: Expression) extends UnaryLogicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  override lazy val strictlyTyped: Try[LogicalPlan] = for {
    n <- limit.strictlyTyped map {
      case e if e.foldable => e
      case _               => throw TypeCheckException("Limit must be a constant")
    }
  } yield if (n sameOrEqual limit) this else copy(limit = n)

  override def nodeCaption: String = s"${getClass.getSimpleName} ${limit.annotatedString}"
}

trait JoinType
case object Inner extends JoinType
case object LeftSemi extends JoinType
case object LeftOuter extends JoinType
case object RightOuter extends JoinType
case object FullOuter extends JoinType

case class Join(
  left: LogicalPlan,
  right: LogicalPlan,
  joinType: JoinType,
  maybeCondition: Option[Predicate]
) extends BinaryLogicalPlan {
  override lazy val output: Seq[Attribute] = joinType match {
    case LeftSemi   => left.output
    case Inner      => left.output ++ right.output
    case LeftOuter  => left.output ++ right.output.map(_.?)
    case RightOuter => left.output.map(_.?) ++ right.output
    case FullOuter  => left.output.map(_.?) ++ right.output.map(_.?)
  }
}

case class Subquery(child: LogicalPlan, alias: String) extends UnaryLogicalPlan {
  override lazy val output: Seq[Attribute] = child.output
}
