package scraper.plans.logical

import scala.reflect.runtime.universe.WeakTypeTag
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import scraper.{Name, Row}
import scraper.annotations.Explain
import scraper.exceptions.{LogicalPlanUnresolvedException, TypeCheckException}
import scraper.expressions._
import scraper.expressions.Cast.widestTypeOf
import scraper.expressions.NamedExpression.newExpressionID
import scraper.expressions.functions._
import scraper.expressions.typecheck.Foldable
import scraper.expressions.windows.{BasicWindowSpec, WindowSpec}
import scraper.plans.logical.patterns.{ResolvedAggregate, Unresolved}
import scraper.plans.QueryPlan
import scraper.plans.logical.Window._
import scraper.reflection.fieldSpecFor
import scraper.trees.TreeNode
import scraper.types.{DataType, IntType, StructType}
import scraper.utils._

case class LogicalPlanMetadata()

trait LogicalPlan extends QueryPlan[LogicalPlan] {
  def metadata: LogicalPlanMetadata

  def withMetadata(metadata: LogicalPlanMetadata): LogicalPlan =
    super.makeCopy(productIterator.toSeq.map { case arg: AnyRef => arg } :+ metadata)

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

  /**
   * Input [[Attribute attributes]] that can be referenced by some expression(s) of this operator
   * but produced by this operator itself rather than the child operator(s).
   */
  lazy val derivedInput: Seq[Attribute] = Nil

  lazy val isWellTyped: Boolean = isResolved && strictlyTyped.isSuccess

  lazy val isStrictlyTyped: Boolean = isWellTyped && (strictlyTyped.get same this)

  override protected def outputStrings: Seq[String] = this match {
    case Unresolved(_) => "?output?" :: Nil
    case _             => super.outputStrings
  }

  def sql: Try[String] = Failure(new UnsupportedOperationException())

  def sqlFragment: Try[String] = sql

  override protected def makeCopy(args: Seq[AnyRef]): LogicalPlan =
    super.makeCopy(args :+ metadata)
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

case class UnresolvedRelation(name: Name)(val metadata: LogicalPlanMetadata = LogicalPlanMetadata())
  extends Relation with UnresolvedLogicalPlan

case class SingleRowRelation(metadata: LogicalPlanMetadata = LogicalPlanMetadata())
  extends Relation {

  override val output: Seq[Attribute] = Nil

  override def sql: Try[String] = Success("")
}

case class LocalRelation(
  @Explain(hidden = true) data: Iterable[Row],
  @Explain(hidden = true) output: Seq[Attribute]
)(val metadata: LogicalPlanMetadata = LogicalPlanMetadata())
  extends MultiInstanceRelation {

  override def newInstance(): LogicalPlan =
    copy(output = output map { _ withID newExpressionID() })(metadata)

  override protected def nestedTrees: Seq[TreeNode[_]] = Nil
}

object LocalRelation {
  def apply[T <: Product: WeakTypeTag](data: Iterable[T]): LocalRelation = {
    val schema = fieldSpecFor[T].dataType match { case t: StructType => t }
    val rows = data.map { product => Row.fromSeq(product.productIterator.toSeq) }
    LocalRelation(rows, schema.toAttributes)()
  }

  def empty(output: Seq[Attribute]): LocalRelation = LocalRelation(Seq.empty[Row], output)()

  def empty(first: Attribute, rest: Attribute*): LocalRelation = empty(first +: rest)
}

case class Distinct(child: LogicalPlan)(val metadata: LogicalPlanMetadata = LogicalPlanMetadata())
  extends UnaryLogicalPlan {

  override def output: Seq[Attribute] = child.output
}

case class Project(child: LogicalPlan, projectList: Seq[NamedExpression])(
  val metadata: LogicalPlanMetadata = LogicalPlanMetadata()
) extends UnaryLogicalPlan {

  assert(projectList.nonEmpty, "Project should have at least one expression")

  override def expressions: Seq[Expression] = projectList

  override lazy val output: Seq[Attribute] = projectList map { _.attr }

  override lazy val sql: Try[String] = this match {
    case ResolvedAggregate(fields, order, condition, keys, childPlan) =>
      for {
        projectListSQL <- trySequence(fields map { _.sql })
        conditionSQL <- trySequence(condition.toSeq map { _.sql })
        orderSQL <- trySequence(order map { _.sql })
        keysSQL <- trySequence(keys map { _.sql })
        childSQL <- childPlan.sqlFragment
      } yield Seq(
        projectListSQL mkString ("SELECT ", ", ", ""),
        childSQL,
        if (keysSQL.isEmpty) "" else keysSQL mkString ("GROUP BY ", ", ", ""),
        if (conditionSQL.isEmpty) "" else conditionSQL mkString ("HAVING ", "", ""),
        if (orderSQL.isEmpty) "" else orderSQL mkString ("ORDER BY ", ", ", "")
      ) filterNot { _.isEmpty } mkString " "

    case Subquery(alias, _: Relation) Project _ =>
      for (projectListSQL <- trySequence(projectList map { _.sql }))
        yield projectListSQL mkString ("SELECT ", ", ", s" FROM $alias")

    case _ =>
      for {
        projectListSQL <- trySequence(projectList map { _.sql })
        childSQL <- child.sqlFragment
      } yield Seq(
        projectListSQL mkString ("SELECT ", ", ", ""),
        childSQL
      ) filterNot { _.isEmpty } mkString " "
  }
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
 * @see [[scraper.plans.logical.analysis.RewriteRenameToProject RewriteRenamesToProjects]]
 */
case class Rename(child: LogicalPlan, aliases: Seq[Name])(
  val metadata: LogicalPlanMetadata = LogicalPlanMetadata()
) extends UnaryLogicalPlan with UnresolvedLogicalPlan

case class Filter(child: LogicalPlan, condition: Expression)(
  val metadata: LogicalPlanMetadata = LogicalPlanMetadata()
) extends UnaryLogicalPlan {

  override lazy val output: Seq[Attribute] = child.output

  override lazy val sql: Try[String] = child match {
    case _: Subquery | _: Relation =>
      for {
        conditionSQL <- condition.sql
        childSQL <- child.sqlFragment
      } yield s"$childSQL WHERE $conditionSQL"

    case _ =>
      Failure(new UnsupportedOperationException())
  }
}

case class Limit(child: LogicalPlan, count: Expression)(
  val metadata: LogicalPlanMetadata = LogicalPlanMetadata()
) extends UnaryLogicalPlan {

  override lazy val output: Seq[Attribute] = child.output

  override lazy val strictlyTyped: Try[LogicalPlan] = for {
    n :: Nil <- (count sameTypeAs IntType andAlso Foldable).enforced orElse Failure(
      new TypeCheckException("Limit must be a constant integer")
    )
  } yield copy(count = n)(metadata)
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

case class Union(left: LogicalPlan, right: LogicalPlan)(
  val metadata: LogicalPlanMetadata = LogicalPlanMetadata()
) extends SetOperator {

  override lazy val output: Seq[Attribute] =
    left.output zip right.output map {
      case (a1, a2) =>
        a1.nullable(a1.isNullable || a2.isNullable)
    }
}

case class Intersect(left: LogicalPlan, right: LogicalPlan)(
  val metadata: LogicalPlanMetadata = LogicalPlanMetadata()
) extends SetOperator {

  override lazy val output: Seq[Attribute] =
    left.output zip right.output map {
      case (a1, a2) =>
        a1.nullable(a1.isNullable && a2.isNullable)
    }
}

case class Except(left: LogicalPlan, right: LogicalPlan)(
  val metadata: LogicalPlanMetadata = LogicalPlanMetadata()
) extends SetOperator {

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
  left: LogicalPlan,
  right: LogicalPlan,
  joinType: JoinType,
  condition: Option[Expression]
)(
  val metadata: LogicalPlanMetadata = LogicalPlanMetadata()
) extends BinaryLogicalPlan {

  override lazy val output: Seq[Attribute] = joinType match {
    case Inner      => left.output ++ right.output
    case LeftOuter  => left.output ++ right.output.map { _.? }
    case RightOuter => left.output.map { _.? } ++ right.output
    case FullOuter  => left.output.map { _.? } ++ right.output.map { _.? }
  }

  def on(condition: Expression): Join = copy(condition = Some(condition))(metadata)

  /**
   * Returns a new [[Join]] operator with a join condition formed by combining all given
   * `predicates`. If `predicates` is empty, simply returns this [[Join]] operator untouched.
   */
  def on(predicates: Seq[Expression]): Join = predicates reduceOption And map on getOrElse this
}

case class Subquery(child: LogicalPlan, alias: Name)(
  val metadata: LogicalPlanMetadata = LogicalPlanMetadata()
) extends UnaryLogicalPlan {

  override lazy val output: Seq[Attribute] = child.output map {
    case a: AttributeRef => a.copy(qualifier = Some(alias))
    case a: Attribute    => a
  }

  override def sql: Try[String] = child match {
    case plan: Relation =>
      for (fieldsSQL <- trySequence(plan.output map { _.sql }))
        yield fieldsSQL mkString ("SELECT ", ", ", s" FROM $alias")

    case plan =>
      for (childSQL <- plan.sqlFragment)
        yield s"($childSQL) AS $alias"
  }

  override def sqlFragment: Try[String] = child match {
    case _: Relation => Success(s"FROM $alias")
    case _           => sql
  }
}

/**
 * A generic unresolved, filtered, and ordered aggregate operator that captures semantics of queries
 * in the form of
 * {{{
 *   SELECT <project-list>
 *   FROM <child-plan>
 *   GROUP BY <keys>
 *   HAVING <condition>
 *   ORDER BY <order>
 * }}}
 * where
 *
 *  - `project-list`, `condition`, and `order` may reference non-window aggregate functions and/or
 *    grouping keys, and
 *  - `project-list` and `order` may reference window functions.
 *
 * This operator is unresolved because it's only allowed during analysis phase to help resolve
 * queries involving aggregation, and must be resolved into a combination of the following resolved
 * operators:
 *
 *  - [[Project]]
 *  - [[Sort]]
 *  - [[Window]]
 *  - [[Filter]]
 *  - [[Aggregate]]
 *
 * @see [[scraper.plans.logical.analysis.RewriteUnresolvedAggregate]]
 */
case class UnresolvedAggregate(
  child: LogicalPlan,
  keys: Seq[Expression],
  projectList: Seq[NamedExpression],
  conditions: Seq[Expression],
  order: Seq[SortOrder]
)(
  val metadata: LogicalPlanMetadata = LogicalPlanMetadata()
) extends UnaryLogicalPlan with UnresolvedLogicalPlan

case class Aggregate(
  child: LogicalPlan,
  keys: Seq[GroupingAlias],
  functions: Seq[AggregationAlias]
)(
  val metadata: LogicalPlanMetadata = LogicalPlanMetadata()
) extends UnaryLogicalPlan {

  override def isResolved: Boolean = super.isResolved

  override lazy val output: Seq[Attribute] = keys ++ functions map { _.attr }

  // Aggregate functions may reference grouping key aliases, which are not produced by `child` but
  // this operator itself.
  override lazy val derivedInput: Seq[Attribute] = keys map { _.attr }
}

case class Sort(child: LogicalPlan, order: Seq[SortOrder])(
  val metadata: LogicalPlanMetadata = LogicalPlanMetadata()
) extends UnaryLogicalPlan {

  override def output: Seq[Attribute] = child.output

  override def sql: Try[String] = for {
    orderSQL <- trySequence(order map { _.sql }) map { _ mkString ", " }
    childSQL <- child.sqlFragment
  } yield s"$childSQL ORDER BY $orderSQL"
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
  child: LogicalPlan,
  name: Name,
  @Explain(hidden = true, nestedTree = true) query: LogicalPlan,
  aliases: Option[Seq[Name]]
)(
  val metadata: LogicalPlanMetadata = LogicalPlanMetadata()
) extends UnaryLogicalPlan {

  override def output: Seq[Attribute] = child.output
}

case class WindowDef(child: LogicalPlan, name: Name, windowSpec: WindowSpec)(
  val metadata: LogicalPlanMetadata = LogicalPlanMetadata()
) extends UnaryLogicalPlan {

  override def output: Seq[Attribute] = child.output
}

case class Window(
  child: LogicalPlan,
  functions: Seq[WindowAlias],
  partitionSpec: Seq[Expression] = Nil,
  orderSpec: Seq[SortOrder] = Nil
)(
  val metadata: LogicalPlanMetadata = LogicalPlanMetadata()
) extends UnaryLogicalPlan {

  override lazy val output: Seq[Attribute] = child.output ++ functions map { _.attr }
}

object Window {
  /**
   * Given a logical `plan` and a list of zero or more window functions, stacks zero or more
   * [[Window]] operators over `plan`.
   */
  def stackWindows(plan: LogicalPlan, windowAliases: Seq[WindowAlias]): LogicalPlan =
    windowBuilders(windowAliases) reduceOption { _ andThen _ } map { _ apply plan } getOrElse plan

  private def windowBuilders(windowAliases: Seq[WindowAlias]): Seq[LogicalPlan => Window] = {
    // Finds out all distinct window specs.
    val windowSpecs = windowAliases.map { _.child.window }.distinct

    // Groups all window functions by their window specs.
    val windowAliasGroups = windowAliases
      .groupBy { _.child.window }
      .mapValues { _ sortBy windowAliases.indexOf }
      .toSeq
      // Sorts them to ensure a deterministic order of all generated `Window` operators.
      .sortBy { case (spec: WindowSpec, _) => windowSpecs indexOf spec }

    // Builds one `Window` operator builder function for each group.
    windowAliasGroups map {
      case (BasicWindowSpec(partitionSpec, orderSpec, _), aliases) =>
        Window(_: LogicalPlan, aliases, partitionSpec, orderSpec)()
    }
  }
}

object LogicalPlan {
  implicit class LogicalPlanDSL(plan: LogicalPlan) {
    def select(projectList: Seq[Expression]): Project =
      Project(plan, projectList map NamedExpression.apply)()

    def select(first: Expression, rest: Expression*): Project = select(first +: rest)

    def rename(aliases: Seq[Name]): Rename = Rename(plan, aliases)()

    def rename(first: Name, rest: Name*): Rename = rename(first +: rest)

    def filter(condition: Expression): Filter = Filter(plan, condition)()

    def filter(predicates: Seq[Expression]): LogicalPlan =
      predicates reduceOption And map filter getOrElse plan

    def limit(n: Expression): Limit = Limit(plan, n)()

    def limit(n: Int): Limit = this limit lit(n)

    def orderBy(order: Seq[Expression]): LogicalPlan =
      if (order.isEmpty) plan else UnresolvedSort(plan, order map SortOrder.apply)()

    def orderBy(first: Expression, rest: Expression*): LogicalPlan =
      this orderBy (first +: rest)

    def sort(order: Seq[Expression]): LogicalPlan =
      if (order.isEmpty) plan else Sort(plan, order map SortOrder.apply)()

    def sort(first: Expression, rest: Expression*): LogicalPlan = this sort (first +: rest)

    def distinct: Distinct = Distinct(plan)()

    def subquery(name: Name): Subquery = Subquery(plan, name)()

    def join(that: LogicalPlan, joinType: JoinType): Join = Join(plan, that, joinType, None)()

    def join(that: LogicalPlan): Join = join(that, Inner)

    def leftJoin(that: LogicalPlan): Join = join(that, LeftOuter)

    def rightJoin(that: LogicalPlan): Join = join(that, RightOuter)

    def outerJoin(that: LogicalPlan): Join = join(that, FullOuter)

    def union(that: LogicalPlan): Union = Union(plan, that)()

    def intersect(that: LogicalPlan): Intersect = Intersect(plan, that)()

    def except(that: LogicalPlan): Except = Except(plan, that)()

    def groupBy(keys: Seq[Expression]): GroupedPlan = GroupedPlan(plan, keys, Nil, Nil)

    def groupBy(first: Expression, rest: Expression*): GroupedPlan = groupBy(first +: rest)

    def aggregate(keys: Seq[GroupingAlias], functions: Seq[AggregationAlias]): Aggregate =
      Aggregate(plan, keys, functions)()

    def window(functions: Seq[WindowAlias]): Window = {
      val Seq(windowSpec) = functions.map { _.child.window }.distinct
      Window(plan, functions, windowSpec.partitionSpec, windowSpec.orderSpec)()
    }

    def window(first: WindowAlias, rest: WindowAlias*): Window = window(first +: rest)

    def windows(functions: Seq[WindowAlias]): LogicalPlan = stackWindows(plan, functions)

    case class GroupedPlan(
      child: LogicalPlan,
      keys: Seq[Expression],
      conditions: Seq[Expression],
      order: Seq[Expression]
    ) {
      def having(conditions: Seq[Expression]): GroupedPlan =
        copy(conditions = this.conditions ++ conditions)

      def having(maybeCondition: Option[Expression]): GroupedPlan =
        copy(conditions = this.conditions ++ maybeCondition.toSeq)

      def having(first: Expression, rest: Expression*): GroupedPlan =
        having(first +: rest)

      def orderBy(order: Seq[Expression]): GroupedPlan = copy(order = order)

      def orderBy(maybeOrder: Option[Seq[Expression]]): GroupedPlan =
        maybeOrder map orderBy getOrElse this

      def orderBy(first: Expression, rest: Expression*): GroupedPlan =
        orderBy(first +: rest)

      def agg(projectList: Seq[Expression]): UnresolvedAggregate = UnresolvedAggregate(
        child, keys, projectList map NamedExpression.apply, conditions, order map SortOrder.apply
      )()

      def agg(first: Expression, rest: Expression*): UnresolvedAggregate = agg(first +: rest)
    }
  }
}
