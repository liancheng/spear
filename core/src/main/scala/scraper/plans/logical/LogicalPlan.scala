package scraper.plans.logical

import scala.reflect.runtime.universe.WeakTypeTag
import scala.util.{Failure, Try}
import scala.util.control.NonFatal

import scraper.{Name, Row}
import scraper.annotations.Explain
import scraper.exceptions.{LogicalPlanUnresolvedException, TypeCheckException}
import scraper.expressions._
import scraper.expressions.Cast.widestTypeOf
import scraper.expressions.NamedExpression.newExpressionID
import scraper.expressions.dsl._
import scraper.plans.QueryPlan
import scraper.plans.logical.dsl._
import scraper.plans.logical.patterns.Unresolved
import scraper.reflection.fieldSpecFor
import scraper.types.{DataType, IntType, StructType}
import scraper.utils._

trait LogicalPlan extends QueryPlan[LogicalPlan] {
  def isResolved: Boolean = (expressions forall (_.isResolved)) && isDeduplicated

  lazy val isDeduplicated: Boolean = isChildrenResolved && (
    children.length < 2 || (children map (_.outputSet) reduce (_ intersectByID _)).isEmpty
  )

  def isChildrenResolved: Boolean = children forall (_.isResolved)

  /**
   * Tries to return a copy of this plan node where all expressions are strictly-typed.
   *
   * @see [[scraper.expressions.Expression.strictlyTyped]]
   */
  def strictlyTyped: Try[LogicalPlan] = Try {
    this transformExpressionsDown {
      case e => e.strictlyTyped.get
    }
  }

  lazy val isWellTyped: Boolean = isResolved && strictlyTyped.isSuccess

  lazy val isStrictlyTyped: Boolean = isWellTyped && (strictlyTyped.get same this)

  override protected def outputStrings: Seq[String] = this match {
    case Unresolved(_) => "?output?" :: Nil
    case _             => super.outputStrings
  }
}

trait UnresolvedLogicalPlan extends LogicalPlan {
  override def output: Seq[Attribute] = throw new LogicalPlanUnresolvedException(this)

  override def isResolved: Boolean = false

  override def strictlyTyped: Try[LogicalPlan] =
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

case class UnresolvedRelation(name: Name) extends Relation with UnresolvedLogicalPlan

case object SingleRowRelation extends Relation {
  override val output: Seq[Attribute] = Nil
}

case class LocalRelation(
  @Explain(hidden = true) data: Iterable[Row],
  @Explain(hidden = true) output: Seq[Attribute]
) extends MultiInstanceRelation {
  override def newInstance(): LogicalPlan = copy(output = output map (_ withID newExpressionID()))
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

case class Limit(child: LogicalPlan, count: Expression) extends UnaryLogicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  override lazy val strictlyTyped: Try[LogicalPlan] = for {
    n <- count.strictlyTyped map {
      case e if e.isFoldable && e.dataType == IntType =>
        Literal(e.evaluated, IntType)

      case e if e.isFoldable && (e.dataType isCastableTo IntType) =>
        Literal(e.evaluated) cast IntType

      case _ =>
        throw new TypeCheckException("Limit must be a constant integer")
    }
  } yield if (n same count) this else copy(count = n)
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
           |$schemaDiff
           |""".stripMargin
      }
    )

  private def alignBranches(branches: Seq[LogicalPlan]): Try[Seq[LogicalPlan]] = {
    // Casts output attributes of a given logical plan to target data types when necessary
    def align(plan: LogicalPlan, targetTypes: Seq[DataType]): LogicalPlan =
      if (plan.schema.fieldTypes == targetTypes) {
        plan
      } else {
        plan select (plan.output zip targetTypes map {
          case (a, t) if a.dataType == t => a
          case (a, t)                    => a cast t as a.name
        })
      }

    for (widenedTypes <- trySequence(branches.map(_.schema.fieldTypes).transpose map widestTypeOf))
      yield branches map (align(_, widenedTypes))
  }

  override lazy val strictlyTyped: Try[LogicalPlan] = {
    for {
      _ <- Try(checkBranchSchemata()) recover {
        case NonFatal(cause) =>
          throw new TypeCheckException(this, cause)
      }

      lhs <- left.strictlyTyped
      rhs <- right.strictlyTyped

      alignedBranches <- alignBranches(lhs :: rhs :: Nil)
    } yield if (sameChildren(alignedBranches)) this else makeCopy(alignedBranches)
  }
}

case class Union(left: LogicalPlan, right: LogicalPlan) extends SetOperator {
  override lazy val output: Seq[Attribute] =
    left.output.zip(right.output).map {
      case (a1, a2) =>
        a1.withNullability(a1.isNullable || a2.isNullable)
    }
}

case class Intersect(left: LogicalPlan, right: LogicalPlan) extends SetOperator {
  override lazy val output: Seq[Attribute] =
    left.output.zip(right.output).map {
      case (a1, a2) =>
        a1.withNullability(a1.isNullable && a2.isNullable)
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
  condition: Option[Expression]
) extends BinaryLogicalPlan {
  override lazy val output: Seq[Attribute] = joinType match {
    case LeftSemi   => left.output
    case Inner      => left.output ++ right.output
    case LeftOuter  => left.output ++ right.output.map(_.?)
    case RightOuter => left.output.map(_.?) ++ right.output
    case FullOuter  => left.output.map(_.?) ++ right.output.map(_.?)
  }

  def on(condition: Expression): Join = copy(condition = Some(condition))

  def onOption(predicates: Seq[Expression]): Join =
    predicates reduceOption And map on getOrElse this
}

case class Subquery(child: LogicalPlan, alias: Name) extends UnaryLogicalPlan {
  override lazy val output: Seq[Attribute] = child.output map {
    case a: AttributeRef => a.copy(qualifier = Some(alias))
    case a: Attribute    => a
  }
}

/**
 * An unresolved, filtered, ordered aggregate operator.
 */
case class UnresolvedAggregate(
  child: LogicalPlan,
  keys: Seq[Expression],
  projectList: Seq[NamedExpression],
  havingConditions: Seq[Expression] = Nil,
  order: Seq[SortOrder] = Nil
) extends UnaryLogicalPlan with UnresolvedLogicalPlan

case class Aggregate(child: LogicalPlan, keys: Seq[GroupingAlias], functions: Seq[AggregationAlias])
  extends UnaryLogicalPlan {

  // TODO Should this return false when `functions` contains distinct aggregate function?
  override def isResolved: Boolean = super.isResolved

  override lazy val output: Seq[Attribute] = (keys ++ functions) map (_.toAttribute)
}

case class Sort(child: LogicalPlan, order: Seq[SortOrder]) extends UnaryLogicalPlan {
  override def output: Seq[Attribute] = child.output
}

case class With(
  child: LogicalPlan,
  name: Name,
  @Explain(hidden = true, nestedTree = true) cteRelation: LogicalPlan
) extends UnaryLogicalPlan {
  override def output: Seq[Attribute] = child.output
}
