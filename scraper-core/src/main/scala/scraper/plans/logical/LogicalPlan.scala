package scraper.plans.logical

import scala.reflect.runtime.universe.WeakTypeTag
import scala.util.{Failure, Try}
import scala.util.control.NonFatal

import scraper.{Name, Row}
import scraper.annotations.Explain
import scraper.exceptions.{LogicalPlanUnresolvedException, TypeCheckException}
import scraper.expressions._
import scraper.expressions.Cast.widestTypeOf
import scraper.expressions.NamedExpression.{named, newExpressionID}
import scraper.expressions.functions._
import scraper.expressions.typecheck.Foldable
import scraper.expressions.windows.{BasicWindowSpec, WindowSpec}
import scraper.plans.QueryPlan
import scraper.plans.logical.Window._
import scraper.reflection.fieldSpecFor
import scraper.trees.TreeNode
import scraper.types.{DataType, IntType, StructType}
import scraper.utils._

trait LogicalPlan extends QueryPlan[LogicalPlan] {
  def isResolved: Boolean = expressions.forall { _.isResolved } && isDeduplicated

  lazy val isDeduplicated: Boolean = children.forall { _.isResolved } && (
    children.length < 2 || children.map { _.outputSet }.reduce { _ intersectByID _ }.isEmpty
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

  lazy val derivedOutput: Seq[Attribute] = Nil

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
  override def newInstance(): LogicalPlan = copy(output = output map { _ withID newExpressionID() })

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

case class Project(projectList: Seq[NamedExpression], child: LogicalPlan)
  extends UnaryLogicalPlan {

  assert(projectList.nonEmpty, "Project should have at least one expression")

  override def expressions: Seq[Expression] = projectList

  override lazy val output: Seq[Attribute] = projectList map { _.attr }
}

/**
 * An unresolved logical plan that helps resolve [[With]] operators. It renames output columns of a
 * given `child` plan to the give list of `aliases`. If the number of output columns of the child
 * plan is less than the length of `aliases`, the rest of the output columns are left untouched.
 *
 * For example, assuming relation `t` has 3 columns, `a`, `b`, and `c`, then the following query
 * plan:
 * {{{
 *   With name=s, aliases=Some([x, y])
 *    | +- Project projectList=[*]
 *    |     +- UnresolvedRelation name=t
 *    +- Project projectList=[x + y]
 *        +- UnresolvedRelation name=s
 * }}}
 * which is equivalent to SQL query:
 * {{{
 *   WITH s (x, y) AS (SELECT * FROM t)
 *   SELECT x + y, c FROM s
 * }}}
 * will be transformed into:
 * {{{
 *   Project projectList=[x + y, c]
 *    +- Subquery alias=s
 *        +- Rename aliases=[x, y]                    (1)
 *            +- Project projectList=[*]              (2)
 *                +- UnresolvedRelation name=t
 * }}}
 * and may be further analyzed into (roughly):
 * {{{
 *   Project projectList=[s.x + s.y, s.c] => [x + y, c]
 *    +- Subquery alias=s => [s.x, s.y, s.c]
 *        +- Project projectList=[t.a AS x, t.b AS y, t.c]
 *            +- Project projectList=[t.a, t.b, t.c] => [t.a, t.b, t.c]
 *                +- Subquery alias=t => [t.a, t.b, t.c]
 *                    +- LocalRelation => [a, b, c]
 * }}}
 * Note that we are not able to construct a proper [[Project]] operator to replace the [[Rename]] at
 * (1) since the number of output columns of the [[Project]] at (2) is unknown.
 *
 * @param child The child plan.
 * @param aliases New column names to apply. Its length should be less than or equal to the number
 *        of output columns of `child`.
 *
 * @see [[With]]
 * @see [[scraper.plans.logical.analysis.RewriteRenamesToProjects RewriteRenamesToProjects]]
 */
case class Rename(aliases: Seq[Name], child: LogicalPlan)
  extends UnaryLogicalPlan with UnresolvedLogicalPlan

case class Filter(condition: Expression, child: LogicalPlan) extends UnaryLogicalPlan {
  override lazy val output: Seq[Attribute] = child.output
}

case class Limit(count: Expression, child: LogicalPlan) extends UnaryLogicalPlan {
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
      left.output.map { _.name } == right.output.map { _.name }, {
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

    for {
      widenedTypes <- trySequence(branches.map { _.schema.fieldTypes }.transpose map widestTypeOf)
    } yield branches map (align(_, widenedTypes))
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
        a1.nullable(a1.isNullable || a2.isNullable)
    }
}

case class Intersect(left: LogicalPlan, right: LogicalPlan) extends SetOperator {
  override lazy val output: Seq[Attribute] =
    left.output zip right.output map {
      case (a1, a2) =>
        a1.nullable(a1.isNullable && a2.isNullable)
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
  joinType: JoinType,
  condition: Option[Expression],
  left: LogicalPlan,
  right: LogicalPlan
) extends BinaryLogicalPlan {
  override lazy val output: Seq[Attribute] = joinType match {
    case Inner      => left.output ++ right.output
    case LeftOuter  => left.output ++ right.output.map { _.? }
    case RightOuter => left.output.map { _.? } ++ right.output
    case FullOuter  => left.output.map { _.? } ++ right.output.map { _.? }
  }

  def on(condition: Expression): Join = copy(condition = Some(condition))

  /**
   * Returns a new [[Join]] operator with a join condition formed by combining all given
   * `predicates`. If `predicates` is empty, simply returns this [[Join]] operator untouched.
   */
  def onOption(predicates: Seq[Expression]): Join =
    predicates reduceOption And map on getOrElse this
}

case class Subquery(alias: Name, child: LogicalPlan) extends UnaryLogicalPlan {
  override lazy val output: Seq[Attribute] = child.output map {
    case a: AttributeRef => a.copy(qualifier = Some(alias))
    case a: Attribute    => a
  }
}

/**
 * A generic aggregate operator that captures semantics of queries in the form of
 * {{{
 *   SELECT <project-list>
 *   FROM <child-plan>
 *   GROUP BY <keys>
 *   HAVING <condition>
 *   ORDER BY <order>
 * }}}
 * where
 *
 *  - `project-list`, `condition`, and `order` may reference non-window aggregate functions and
 *    grouping keys, and
 *  - `project-list` and `order` may also reference window functions.
 *
 * This operator is unresolved because it's only allowed during analysis phase to help resolve
 * queries involving aggregation, and must be rewritten into combinations of other resolved
 * operators.
 */
case class UnresolvedAggregate(
  keys: Seq[Expression],
  projectList: Seq[NamedExpression],
  conditions: Seq[Expression] = Nil,
  order: Seq[SortOrder] = Nil,
  child: LogicalPlan
) extends UnaryLogicalPlan with UnresolvedLogicalPlan

case class Aggregate(child: LogicalPlan, keys: Seq[GroupingAlias], functions: Seq[AggregationAlias])
  extends UnaryLogicalPlan {

  override def isResolved: Boolean = super.isResolved

  override lazy val output: Seq[Attribute] = keys ++ functions map { _.attr }

  override lazy val derivedOutput: Seq[Attribute] = keys ++ functions map { _.attr }
}

case class Sort(order: Seq[SortOrder], child: LogicalPlan) extends UnaryLogicalPlan {
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
 *    | +- UnresolvedRelation name=x
 *    +- With name=s0
 *        | +- UnresolvedRelation name=y
 *        +- Join
 *            +- UnresolvedRelation name=s0
 *            +- UnresolvedRelation name=s1
 * }}}
 */
case class With(
  name: Name,
  @Explain(hidden = true, nestedTree = true) query: LogicalPlan,
  aliases: Option[Seq[Name]],
  child: LogicalPlan
) extends UnaryLogicalPlan {
  override def output: Seq[Attribute] = child.output
}

case class WindowDef(child: LogicalPlan, name: Name, windowSpec: WindowSpec)
  extends UnaryLogicalPlan {

  override def output: Seq[Attribute] = child.output
}

case class Window(
  functions: Seq[WindowAlias],
  partitionSpec: Seq[Expression] = Nil,
  orderSpec: Seq[SortOrder] = Nil,
  child: LogicalPlan
) extends UnaryLogicalPlan {
  override lazy val output: Seq[Attribute] = child.output ++ functions map { _.attr }
}

object Window {
  /**
   * Given a logical `plan` and a list of zero or more window functions, stacks zero or more
   * [[Window]] operators over `plan`.
   */
  def stackWindowsOption(plan: LogicalPlan, windowAliases: Seq[WindowAlias]): LogicalPlan =
    windowBuilders(windowAliases) reduceOption { _ andThen _ } map { _ apply plan } getOrElse plan

  /**
   * Given a logical `plan` and a list of one or more window functions, stacks one or more
   * [[Window]] operators over `plan`.
   */
  def stackWindows(plan: LogicalPlan, windowAliases: Seq[WindowAlias]): Window = {
    assert(windowAliases.nonEmpty)
    windowBuilders(windowAliases) reduce { _ andThen _ } apply plan
  }

  private def windowBuilders(windowAliases: Seq[WindowAlias]): Seq[LogicalPlan => Window] = {
    // Finds out all distinct window specs.
    val windowSpecs = windowAliases.map { _.child.window }.distinct

    // Groups all window functions by their window specs. We are doing sorts here so that it would
    // be easier to reason about the order of all the generated `Window` operators.
    val windowAliasGroups = windowAliases
      .groupBy { _.child.window }
      .mapValues { _ sortBy windowAliases.indexOf }
      .toSeq
      .sortBy { case (spec: WindowSpec, _) => windowSpecs indexOf spec }

    // Builds one `Window` operator builder function for each group.
    windowAliasGroups map {
      case (BasicWindowSpec(partitionSpec, orderSpec, _), aliases) =>
        Window(aliases, partitionSpec, orderSpec, _: LogicalPlan)
    }
  }
}

object LogicalPlan {
  implicit class LogicalPlanDSL(plan: LogicalPlan) {
    def select(projectList: Seq[Expression]): Project = Project(projectList map named, plan)

    def select(first: Expression, rest: Expression*): Project = select(first +: rest)

    def rename(aliases: Seq[Name]): Rename = Rename(aliases, plan)

    def rename(first: Name, rest: Name*): Rename = rename(first +: rest)

    def filter(condition: Expression): Filter = Filter(condition, plan)

    def filterOption(predicates: Seq[Expression]): LogicalPlan =
      predicates reduceOption And map filter getOrElse plan

    def limit(n: Expression): Limit = Limit(n, plan)

    def limit(n: Int): Limit = this limit lit(n)

    def orderBy(order: Seq[Expression]): Sort = Sort(order map {
      case e: SortOrder => e
      case e            => e.asc
    }, plan)

    def orderBy(first: Expression, rest: Expression*): Sort = this orderBy (first +: rest)

    def orderByOption(order: Seq[Expression]): LogicalPlan =
      if (order.nonEmpty) orderBy(order) else plan

    def distinct: Distinct = Distinct(plan)

    def subquery(name: Name): Subquery = Subquery(name, plan)

    def join(that: LogicalPlan, joinType: JoinType): Join = Join(joinType, None, plan, that)

    def join(that: LogicalPlan): Join = Join(Inner, None, plan, that)

    def leftJoin(that: LogicalPlan): Join = Join(LeftOuter, None, plan, that)

    def rightJoin(that: LogicalPlan): Join = Join(RightOuter, None, plan, that)

    def outerJoin(that: LogicalPlan): Join = Join(FullOuter, None, plan, that)

    def union(that: LogicalPlan): Union = Union(plan, that)

    def intersect(that: LogicalPlan): Intersect = Intersect(plan, that)

    def except(that: LogicalPlan): Except = Except(plan, that)

    def groupBy(keys: Seq[Expression]): UnresolvedAggregateBuilder =
      new UnresolvedAggregateBuilder(keys)

    def groupBy(first: Expression, rest: Expression*): UnresolvedAggregateBuilder =
      groupBy(first +: rest)

    def agg(projectList: Seq[Expression]): UnresolvedAggregate = this groupBy Nil agg projectList

    def agg(first: Expression, rest: Expression*): UnresolvedAggregate = agg(first +: rest)

    def resolvedGroupBy(keys: Seq[GroupingAlias]): AggregateBuilder =
      new AggregateBuilder(keys)

    def resolvedGroupBy(first: GroupingAlias, rest: GroupingAlias*): AggregateBuilder =
      resolvedGroupBy(first +: rest)

    def resolvedAgg(functions: Seq[AggregationAlias]): Aggregate =
      plan resolvedGroupBy Nil agg functions

    def resolvedAgg(first: AggregationAlias, rest: AggregationAlias*): Aggregate =
      resolvedAgg(first +: rest)

    def window(functions: Seq[WindowAlias]): Window = {
      val Seq(windowSpec) = functions.map { _.child.window }.distinct
      Window(functions, windowSpec.partitionSpec, windowSpec.orderSpec, plan)
    }

    def window(first: WindowAlias, rest: WindowAlias*): Window = window(first +: rest)

    def windows(functions: Seq[WindowAlias]): LogicalPlan = stackWindowsOption(plan, functions)

    def windowsOption(functions: Seq[WindowAlias]): LogicalPlan =
      stackWindowsOption(plan, functions)

    class UnresolvedAggregateBuilder(keys: Seq[Expression]) {
      def agg(projectList: Seq[Expression]): UnresolvedAggregate =
        UnresolvedAggregate(keys, projectList map named, Nil, Nil, plan)

      def agg(first: Expression, rest: Expression*): UnresolvedAggregate = agg(first +: rest)
    }

    class AggregateBuilder(keys: Seq[GroupingAlias]) {
      def agg(functions: Seq[AggregationAlias]): Aggregate = Aggregate(plan, keys, functions)

      def agg(first: AggregationAlias, rest: AggregationAlias*): Aggregate = agg(first +: rest)
    }
  }
}
