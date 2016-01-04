package scraper.plans.logical

import scala.reflect.runtime.universe.WeakTypeTag
import scala.util.Try
import scalaz.Scalaz._

import scraper.exceptions.{LogicalPlanUnresolved, TypeCheckException}
import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.QueryPlan
import scraper.reflection.fieldSpecFor
import scraper.types.{IntegralType, StructType}
import scraper.utils._
import scraper.{Context, Row}

trait LogicalPlan extends QueryPlan[LogicalPlan] {
  def resolved: Boolean = (expressions forall (_.resolved)) && childrenResolved

  def childrenResolved: Boolean = children forall (_.resolved)

  def strictlyTypedForm: Try[LogicalPlan] = Try {
    this transformExpressionsDown {
      case e => e.strictlyTypedForm.get
    }
  }

  lazy val strictlyTyped: Boolean = resolved && (strictlyTypedForm.get sameOrEqual this)

  def childrenStrictlyTyped: Boolean = children forall (_.strictlyTyped)

  def select(projections: Seq[Expression]): LogicalPlan =
    Project(this, projections.zipWithIndex map {
      case (UnresolvedAttribute("*"), _) => Star
      case (e: NamedExpression, _)       => e
      case (e, ordinal)                  => e as s"col$ordinal"
    })

  def select(first: Expression, rest: Expression*): LogicalPlan = select(first +: rest)

  def filter(condition: Expression): LogicalPlan = Filter(this, condition)

  def limit(n: Expression): LogicalPlan = Limit(this, n)

  def limit(n: Int): LogicalPlan = this limit lit(n)

  def orderBy(order: Seq[SortOrder]): Sort = Sort(this, order)

  def orderBy(first: SortOrder, rest: SortOrder*): Sort = this orderBy (first +: rest)

  override def nodeCaption: String = if (resolved) {
    super.nodeCaption
  } else {
    s"$nodeName args=$argsString"
  }
}

trait UnresolvedLogicalPlan extends LogicalPlan {
  override def output: Seq[Attribute] = throw new LogicalPlanUnresolved(this)

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

case class LocalRelation(data: Iterable[Row], output: Seq[Attribute])
  extends LeafLogicalPlan {

  override def nodeCaption: String = s"$nodeName output=$outputString"
}

object LocalRelation {
  def apply[T <: Product: WeakTypeTag](data: Iterable[T]): LocalRelation = {
    val schema = fieldSpecFor[T].dataType match { case t: StructType => t }
    val rows = data.map { product => Row.fromSeq(product.productIterator.toSeq) }
    LocalRelation(rows, schema.toAttributes)
  }
}

case class Project(child: LogicalPlan, projections: Seq[NamedExpression])
  extends UnaryLogicalPlan {

  assert(projections.nonEmpty, "Project should have at least one expression")

  override def expressions: Seq[Expression] = projections

  override lazy val output: Seq[Attribute] = projections map (_.toAttribute)
}

case class Filter(child: LogicalPlan, condition: Expression) extends UnaryLogicalPlan {
  override lazy val output: Seq[Attribute] = child.output
}

case class Limit(child: LogicalPlan, limit: Expression) extends UnaryLogicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  override lazy val strictlyTypedForm: Try[LogicalPlan] = for {
    n <- limit.strictlyTypedForm map {
      case IntegralType(e) if e.foldable            => e
      case IntegralType.Implicitly(e) if e.foldable => e
      case _ =>
        throw new TypeCheckException("Limit must be an integral constant")
    }
  } yield if (n sameOrEqual limit) this else copy(limit = n)
}

trait JoinType {
  def sql: String
}

case object Inner extends JoinType {
  override def sql: String = "INNER"
}

case object LeftSemi extends JoinType {
  override def sql: String = "LEFT SEMI"
}

case object LeftOuter extends JoinType {
  override def sql: String = "LEFT OUTER"
}

case object RightOuter extends JoinType {
  override def sql: String = "RIGHT OUTER"
}

case object FullOuter extends JoinType {
  override def sql: String = "FULL OUTER"
}

case class Join(
  left: LogicalPlan,
  right: LogicalPlan,
  joinType: JoinType,
  maybeCondition: Option[Expression]
) extends BinaryLogicalPlan {
  override lazy val output: Seq[Attribute] = joinType match {
    case LeftSemi   => left.output
    case Inner      => left.output ++ right.output
    case LeftOuter  => left.output ++ right.output.map(_.?)
    case RightOuter => left.output.map(_.?) ++ right.output
    case FullOuter  => left.output.map(_.?) ++ right.output.map(_.?)
  }

  def on(condition: Expression): Join = copy(maybeCondition = Some(condition))
}

case class Subquery(child: LogicalPlan, alias: String, fromTable: Boolean = false)
  extends UnaryLogicalPlan {

  override lazy val output: Seq[Attribute] = child.output
}

case class Aggregate(
  child: LogicalPlan,
  groupingExpressions: Seq[Expression],
  aggregateExpressions: Seq[NamedExpression]
) extends UnaryLogicalPlan {

  override lazy val output: Seq[Attribute] = aggregateExpressions map (_.toAttribute)
}

case class Sort(child: LogicalPlan, order: Seq[SortOrder]) extends UnaryLogicalPlan {
  override def output: Seq[Attribute] = child.output
}
