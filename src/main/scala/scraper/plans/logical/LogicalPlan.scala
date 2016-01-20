package scraper.plans.logical

import scala.reflect.runtime.universe.WeakTypeTag
import scala.util.Try
import scalaz.Scalaz._

import scraper.Row
import scraper.exceptions.{LogicalPlanUnresolved, TypeCheckException}
import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.QueryPlan
import scraper.plans.logical.Optimizer.{ReduceCasts, ReduceProjects}
import scraper.reflection.fieldSpecFor
import scraper.types.{DataType, IntegralType, StructType}
import scraper.utils._

trait LogicalPlan extends QueryPlan[LogicalPlan] {
  def resolved: Boolean = (expressions forall (_.resolved)) && childrenResolved

  def childrenResolved: Boolean = children forall (_.resolved)

  def strictlyTypedForm: Try[LogicalPlan] = Try {
    this transformExpressionsDown {
      case e => e.strictlyTypedForm.get
    }
  }

  lazy val strictlyTyped: Boolean = resolved && (strictlyTypedForm.get sameOrEqual this)

  def select(projections: Seq[Expression]): Project =
    Project(this, projections.zipWithIndex map {
      case (UnresolvedAttribute("*"), _) => Star
      case (e: NamedExpression, _)       => e
      case (e, ordinal)                  => e as (e.sql getOrElse s"col$ordinal")
    })

  def select(first: Expression, rest: Expression*): Project = select(first +: rest)

  def filter(condition: Expression): Filter = Filter(this, condition)

  def where(condition: Expression): Filter = filter(condition)

  def limit(n: Expression): Limit = Limit(this, n)

  def limit(n: Int): Limit = this limit lit(n)

  def orderBy(order: Seq[SortOrder]): Sort = Sort(this, order)

  def orderBy(first: SortOrder, rest: SortOrder*): Sort = this orderBy (first +: rest)

  def subquery(name: String): Subquery = Subquery(this, name)

  def join(that: LogicalPlan): LogicalPlan = Join(this, that, Inner, None)

  def leftJoin(that: LogicalPlan): Join = Join(this, that, LeftOuter, None)

  def rightJoin(that: LogicalPlan): Join = Join(this, that, RightOuter, None)

  def outerJoin(that: LogicalPlan): Join = Join(this, that, FullOuter, None)

  def union(that: LogicalPlan): Union = Union(this, that)

  def intersect(that: LogicalPlan): Intersect = Intersect(this, that)

  def except(that: LogicalPlan): Except = Except(this, that)

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

case object OneRowRelation extends LeafLogicalPlan {
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

case class Distinct(child: LogicalPlan) extends UnaryLogicalPlan {
  override def output: Seq[Attribute] = child.output
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

trait SetOperator extends BinaryLogicalPlan {
  require(
    left.output.map(_.name) == right.output.map(_.name),
    s"""$nodeName branches have incompatible schemata.  Left branch:
       |
       |${left.schema.prettyTree}
       |
       |right branch:
       |
       |${right.schema.prettyTree}
     """.stripMargin
  )

  override lazy val strictlyTypedForm: Try[LogicalPlan] = {
    def widen(branch: LogicalPlan, types: Seq[DataType]): LogicalPlan = {
      val projectList = (branch.output, types).zipped.map(_ cast _)
      // Removes redundant casts and projects
      (ReduceCasts andThen ReduceProjects)(branch select projectList)
    }

    for {
      lhs <- left.strictlyTypedForm
      rhs <- right.strictlyTypedForm

      lhsTypes = left.schema.fieldTypes
      rhsTypes = right.schema.fieldTypes

      widenedTypes <- sequence((lhsTypes, rhsTypes).zipped map (_ widest _))

      widenedLhs = widen(lhs, widenedTypes)
      widenedRhs = widen(rhs, widenedTypes)

      newChildren = widenedLhs :: widenedRhs :: Nil
    } yield if (sameChildren(newChildren)) this else makeCopy(newChildren)
  }
}

case class Union(left: LogicalPlan, right: LogicalPlan) extends SetOperator {
  override lazy val output: Seq[Attribute] =
    left.output.zip(right.output).map {
      case (a1, a2) =>
        a1.withNullability(a1.nullable || a2.nullable)
    }
}

case class Intersect(left: LogicalPlan, right: LogicalPlan) extends SetOperator {
  override lazy val output: Seq[Attribute] =
    left.output.zip(right.output).map {
      case (a1, a2) =>
        a1.withNullability(a1.nullable && a2.nullable)
    }
}

case class Except(left: LogicalPlan, right: LogicalPlan) extends SetOperator {
  override lazy val output: Seq[Attribute] = left.output
}

sealed trait JoinType {
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

  def on(condition: Expression): Join = copy(maybeCondition = condition.some)
}

case class Subquery(child: LogicalPlan, alias: String) extends UnaryLogicalPlan {

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
