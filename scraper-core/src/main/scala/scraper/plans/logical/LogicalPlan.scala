package scraper.plans.logical

import scala.reflect.runtime.universe.WeakTypeTag
import scala.util.{Failure, Try}
import scala.util.control.NonFatal

import scraper.{Name, Row}
import scraper.annotations.Explain
import scraper.exceptions.{LogicalPlanUnresolvedException, TypeCheckException}
import scraper.expressions._
import scraper.expressions.AutoAlias.named
import scraper.expressions.Cast.widestTypeOf
import scraper.expressions.NamedExpression.newExpressionID
import scraper.expressions.functions._
import scraper.expressions.typecheck.Foldable
import scraper.plans.QueryPlan
import scraper.plans.logical.analysis.WindowAnalysis.stackWindows
import scraper.reflection.fieldSpecFor
import scraper.trees.TreeNode
import scraper.types.{DataType, IntType, StructType}
import scraper.utils._

trait LogicalPlan extends QueryPlan[LogicalPlan] {
  def isResolved: Boolean = (expressions forall (_.isResolved)) && isDeduplicated

  lazy val isDeduplicated: Boolean = children.forall(_.isResolved) && (
    children.length < 2 || (children map (_.outputSet) reduce (_ intersectByID _)).isEmpty
  )

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

  override protected def nestedTrees: Seq[TreeNode[_]] = Nil
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
    n :: Nil <- (count sameTypeAs IntType andAlso Foldable).enforced orElse Failure(
      new TypeCheckException("Limit must be a constant integer")
    )
  } yield copy(count = n)
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

  override lazy val strictlyTyped: Try[LogicalPlan] = for {
    _ <- Try(checkBranchSchemata()) recover {
      case NonFatal(cause) =>
        throw new TypeCheckException(this, cause)
    }

    lhs <- left.strictlyTyped
    rhs <- right.strictlyTyped

    alignedBranches <- alignBranches(lhs :: rhs :: Nil)
  } yield makeCopy(alignedBranches)
}

case class Union(left: LogicalPlan, right: LogicalPlan) extends SetOperator {
  override lazy val output: Seq[Attribute] =
    left.output zip right.output map {
      case (a1, a2) =>
        a1.withNullability(a1.isNullable || a2.isNullable)
    }
}

case class Intersect(left: LogicalPlan, right: LogicalPlan) extends SetOperator {
  override lazy val output: Seq[Attribute] =
    left.output zip right.output map {
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

  /**
   * Returns a new [[Join]] operator with a join condition formed by combining all given
   * `predicates`. If `predicates` is empty, simply returns this [[Join]] operator untouched.
   */
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

  override def isResolved: Boolean = super.isResolved

  override lazy val output: Seq[Attribute] = (keys ++ functions) map (_.toAttribute)
}

case class Sort(child: LogicalPlan, order: Seq[SortOrder]) extends UnaryLogicalPlan {
  override def output: Seq[Attribute] = child.output
}

/**
 * A logical plan operator used to implement CTE. Note that one [[With]] operator only holds a
 * single CTE relation. Queries involving multiple CTE relations are represented by nested [[With]]
 * operators. E.g., the following query
 * {{{
 *   WITH s0 AS (SELECT * FROM x), s1 AS (SELECT * FROM y)
 *   SELECT * FROM s0, s1
 * }}}
 * is translated into a query plan like this:
 * {{{
 *   With name=s1
 *    | +- Relation x
 *    +- With name=s0
 *        | +- Relation y
 *        +- Join
 *            +- Relation s0
 *            +- Relation s1
 * }}}
 */
case class With(
  child: LogicalPlan,
  name: Name,
  @Explain(hidden = true, nestedTree = true) cteRelation: LogicalPlan
) extends UnaryLogicalPlan {
  override def output: Seq[Attribute] = child.output
}

case class Window(
  child: LogicalPlan,
  functions: Seq[WindowAlias],
  partitionSpec: Seq[Expression],
  orderSpec: Seq[SortOrder]
) extends UnaryLogicalPlan {
  override def output: Seq[Attribute] = child.output ++ functions.map(_.toAttribute)

  def partitionBy(partitionSpec: Seq[Expression]): Window = copy(partitionSpec = partitionSpec)

  def orderBy(orderSpec: Seq[SortOrder]): Window = copy(orderSpec = orderSpec)
}

object LogicalPlan {
  implicit class LogicalPlanDSL(plan: LogicalPlan) {
    def select(projectList: Seq[Expression]): Project = Project(plan, projectList map named)

    def select(first: Expression, rest: Expression*): Project = select(first +: rest)

    def filter(condition: Expression): Filter = Filter(plan, condition)

    def filterOption(predicates: Seq[Expression]): LogicalPlan =
      predicates reduceOption And map filter getOrElse plan

    def where(condition: Expression): Filter = filter(condition)

    def having(condition: Expression): Filter = filter(condition)

    def limit(n: Expression): Limit = Limit(plan, n)

    def limit(n: Int): Limit = this limit lit(n)

    def orderBy(order: Seq[SortOrder]): Sort = Sort(plan, order)

    def orderBy(first: SortOrder, rest: SortOrder*): Sort = this orderBy (first +: rest)

    def orderByOption(order: Seq[SortOrder]): LogicalPlan =
      if (order.nonEmpty) orderBy(order) else plan

    def distinct: Distinct = Distinct(plan)

    def subquery(name: Name): Subquery = Subquery(plan, name)

    def join(that: LogicalPlan, joinType: JoinType): Join = Join(plan, that, joinType, None)

    def join(that: LogicalPlan): Join = Join(plan, that, Inner, None)

    def leftSemiJoin(that: LogicalPlan): Join = Join(plan, that, LeftSemi, None)

    def leftJoin(that: LogicalPlan): Join = Join(plan, that, LeftOuter, None)

    def rightJoin(that: LogicalPlan): Join = Join(plan, that, RightOuter, None)

    def outerJoin(that: LogicalPlan): Join = Join(plan, that, FullOuter, None)

    def union(that: LogicalPlan): Union = Union(plan, that)

    def intersect(that: LogicalPlan): Intersect = Intersect(plan, that)

    def except(that: LogicalPlan): Except = Except(plan, that)

    def groupBy(keys: Seq[Expression]): UnresolvedAggregateBuilder =
      new UnresolvedAggregateBuilder(plan, keys)

    def groupBy(first: Expression, rest: Expression*): UnresolvedAggregateBuilder =
      new UnresolvedAggregateBuilder(plan, first +: rest)

    def agg(projectList: Seq[Expression]): UnresolvedAggregate = this groupBy Nil agg projectList

    def agg(first: Expression, rest: Expression*): UnresolvedAggregate = agg(first +: rest)

    def resolvedGroupBy(keys: Seq[GroupingAlias]): ResolvedAggregateBuilder =
      new ResolvedAggregateBuilder(plan, keys)

    def resolvedGroupBy(first: GroupingAlias, rest: GroupingAlias*): ResolvedAggregateBuilder =
      resolvedGroupBy(first +: rest)

    def resolvedAgg(functions: Seq[AggregationAlias]): Aggregate =
      plan resolvedGroupBy Nil agg functions

    def resolvedAgg(first: AggregationAlias, rest: AggregationAlias*): Aggregate =
      resolvedAgg(first +: rest)

    def windowOption(functions: Seq[WindowAlias]): LogicalPlan = stackWindows(plan, functions)
  }

  class UnresolvedAggregateBuilder(plan: LogicalPlan, keys: Seq[Expression]) {
    def agg(projectList: Seq[Expression]): UnresolvedAggregate =
      UnresolvedAggregate(plan, keys, projectList map named)

    def agg(first: Expression, rest: Expression*): UnresolvedAggregate = agg(first +: rest)
  }

  class ResolvedAggregateBuilder(plan: LogicalPlan, keys: Seq[GroupingAlias]) {
    def agg(functions: Seq[AggregationAlias]): Aggregate = Aggregate(plan, keys, functions)

    def agg(first: AggregationAlias, rest: AggregationAlias*): Aggregate = agg(first +: rest)
  }
}
