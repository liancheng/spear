package scraper.plans.logical

import scala.reflect.runtime.universe.WeakTypeTag
import scala.util.{Failure, Try}
import scalaz.Scalaz._

import scraper.Row
import scraper.exceptions.{LogicalPlanUnresolvedException, TypeCheckException}
import scraper.expressions.Cast.promoteDataType
import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.QueryPlan
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

  lazy val wellTyped: Boolean = resolved && strictlyTypedForm.isSuccess

  lazy val strictlyTyped: Boolean = wellTyped && (strictlyTypedForm.get sameOrEqual this)

  override protected def outputStrings: Seq[String] =
    if (resolved) super.outputStrings else "???" :: Nil

  def select(projectList: Seq[Expression]): Project =
    Project(this, projectList map {
      case UnresolvedAttribute("*") => Star
      case e: NamedExpression       => e
      case e                        => e as (e.sql getOrElse "?column?")
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
}

trait UnresolvedLogicalPlan extends LogicalPlan {
  override def output: Seq[Attribute] = throw new LogicalPlanUnresolvedException(this)

  override def resolved: Boolean = false

  override def strictlyTypedForm: Try[LogicalPlan] =
    Failure(new LogicalPlanUnresolvedException(this))
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

trait Relation extends LeafLogicalPlan

trait MultiInstanceRelation extends Relation {
  def newInstance(): LogicalPlan
}

case class UnresolvedRelation(name: String) extends Relation with UnresolvedLogicalPlan

case object SingleRowRelation extends Relation {
  override val output: Seq[Attribute] = Nil
}

case class LocalRelation(data: Iterable[Row], output: Seq[Attribute])
  extends MultiInstanceRelation {

  override protected def argsStrings: Seq[String] = Nil

  override def newInstance(): LogicalPlan = copy(output = output map (_.newInstance()))
}

object LocalRelation {
  def apply[T <: Product: WeakTypeTag](data: Iterable[T]): LocalRelation = {
    val schema = fieldSpecFor[T].dataType match { case t: StructType => t }
    val rows = data.map { product => Row.fromSeq(product.productIterator.toSeq) }
    LocalRelation(rows, schema.toAttributes)
  }

  def empty(output: Attribute*): LocalRelation = LocalRelation(Seq.empty[Row], output)

  def empty(schema: StructType): LocalRelation = LocalRelation(Seq.empty[Row], schema.toAttributes)
}

case class Distinct(child: LogicalPlan) extends UnaryLogicalPlan {
  override def output: Seq[Attribute] = child.output
}

case class Project(child: LogicalPlan, projectList: Seq[NamedExpression])
  extends UnaryLogicalPlan {

  assert(projectList.nonEmpty, "Project should have at least one expression")

  override def expressions: Seq[Expression] = projectList

  override lazy val output: Seq[Attribute] = projectList map (_.toAttribute)
}

case class Filter(child: LogicalPlan, condition: Expression) extends UnaryLogicalPlan {
  override lazy val output: Seq[Attribute] = child.output
}

case class Limit(child: LogicalPlan, limit: Expression) extends UnaryLogicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  override lazy val strictlyTypedForm: Try[LogicalPlan] = for {
    n <- limit.strictlyTypedForm map {
      case IntegralType(e) if e.foldable =>
        Literal(e.evaluated)

      case IntegralType.Implicitly(e) if e.foldable =>
        Literal(promoteDataType(e, IntegralType.defaultType).evaluated)

      case _ =>
        throw new TypeCheckException("Limit must be an integral constant")
    }
  } yield if (n sameOrEqual limit) this else copy(limit = n)
}

trait SetOperator extends BinaryLogicalPlan {
  private def checkBranchSchemata(): Unit =
    require(
      left.output.map(_.name) == right.output.map(_.name), {
        val schemaDiff = sideBySide(
          s"""Left branch
             |${left.schema.prettyTree}
             |""".stripMargin,

          s"""Right branch
             |${right.schema.prettyTree}
             |""".stripMargin,

          withHeader = true
        )

        s"""$nodeName branches have incompatible schemata:
           |
           |$schemaDiff
           |""".stripMargin
      }
    )

  override lazy val strictlyTypedForm: Try[LogicalPlan] = {
    def widen(branch: LogicalPlan, types: Seq[DataType]): LogicalPlan = {
      if ((branch.schema.fieldTypes, types).zipped.forall(_ == _)) {
        branch
      } else {
        branch select branch.output.zip(types).map {
          case (a, t) if a.dataType == t => a
          case (a, t)                    => a cast t as a.name
        }
      }
    }

    for {
      _ <- Try(checkBranchSchemata()).recover {
        case cause: Throwable =>
          throw new TypeCheckException(this, cause)
      }

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

  lazy val selfJoinResolved: Boolean = (left.outputSet & right.outputSet).isEmpty

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
