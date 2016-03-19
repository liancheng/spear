package scraper.expressions

import scala.util.{Failure, Try}

import scraper.Row
import scraper.exceptions._
import scraper.expressions.dsl.ExpressionDSL
import scraper.trees.TreeNode
import scraper.types.DataType
import scraper.utils._

trait Expression extends TreeNode[Expression] with ExpressionDSL {
  def isFoldable: Boolean = children forall (_.isFoldable)

  def isNullable: Boolean = children exists (_.isNullable)

  def isPure: Boolean = children forall (_.isPure)

  def isResolved: Boolean = isChildrenResolved

  def isChildrenResolved: Boolean = children forall (_.isResolved)

  def references: Set[Attribute] = children.toSet flatMap ((_: Expression).references)

  /**
   * Tries to return a strictly typed copy of this [[Expression]].  If this [[Expression]] is
   * already strictly typed, it's returned untouched.  If this [[Expression]] cannot be converted to
   * strictly typed form, we say it doesn't type check, and a `Failure` containing an exception with
   * detailed type check error message is returned.
   *
   * Any legal [[Expression]] `e` must be either strictly typed or well typed:
   *
   *  1. Strictly typed: `e` is strictly typed iff
   *
   *     - `e` is resolved, and
   *     - all child expressions of `e` are strictly typed, and
   *     - all child expressions of `e` immediately meet all type requirements of `e`.
   *
   *  2. Well typed: `e` is well typed iff
   *
   *     - `e` is resolved, and
   *     - all child expressions of `e` are well typed, and
   *     - all child expressions of `e` can meet all type requirements of `e` by applying at most
   *       one implicit cast(s).
   *
   * For example, say attribute `a` is an attribute of type `LONG`, then `a + 1` is well typed
   * because:
   *
   *  - Operator `+` requires both branches share the same type, while
   *  - literal `1` is of type `INT`, but can be implicitly casted to `LONG`.
   *
   * On the other hand, `a + CAST(1 AS LONG)` is strictly typed because both branches are of type
   * `LONG`.
   */
  def strictlyTyped: Try[Expression] = Try {
    this transformChildrenUp {
      case e: Expression => e.strictlyTyped.get
    }
  }

  /**
   * Indicates whether this [[Expression]] is strictly typed.
   *
   * @see [[strictlyTyped]]
   */
  lazy val isStrictlyTyped: Boolean = isWellTyped && (strictlyTyped.get same this)

  /**
   * Returns `value` if this [[Expression]] is strictly typed, otherwise throws a
   * [[scraper.exceptions.TypeCheckException TypeCheckException]].
   *
   * @see [[strictlyTyped]]
   */
  @throws[TypeCheckException]("If this expression is not strictly typed")
  protected def whenStrictlyTyped[T](value: => T): T =
    if (isStrictlyTyped) value else throw new TypeCheckException(this)

  /**
   * Indicates whether this [[Expression]] is well typed.
   *
   * @see [[strictlyTyped]]
   */
  lazy val isWellTyped: Boolean = isResolved && strictlyTyped.isSuccess

  /**
   * Returns `value` if this [[Expression]] is strictly typed, otherwise throws a
   * [[scraper.exceptions.TypeCheckException TypeCheckException]].
   *
   * @see [[strictlyTyped]]
   */
  @throws[TypeCheckException]("If this expression is not well typed")
  protected def whenWellTyped[T](value: => T): T =
    if (isWellTyped) value else throw new TypeCheckException(this)

  /**
   * Returns the data type of this [[Expression]] if it's well typed, or throws
   * [[scraper.exceptions.AnalysisException AnalysisException]] (or one of its subclasses) if it's
   * not.
   *
   * By default, this method performs type checking and delegates to [[strictDataType]] if this
   * [[Expression]] is well typed.  But subclasses may override this method directly if the
   * expression data type is fixed (e.g. comparisons and predicates).
   *
   * @see [[strictlyTyped]]
   */
  def dataType: DataType = whenWellTyped(strictlyTyped.get.strictDataType)

  /**
   * Returns the data type of a strictly typed [[Expression]]. This method is only called when this
   * [[Expression]] is strictly typed.
   *
   * @see [[strictlyTyped]]
   */
  protected def strictDataType: DataType = throw new BrokenContractException(
    s"${getClass.getName} must override either dataType or strictDataType."
  )

  def evaluate(input: Row): Any

  def evaluated: Any = evaluate(null)

  def childrenTypes: Seq[DataType] = children.map(_.dataType)

  override def nodeCaption: String = getClass.getSimpleName

  /**
   * A template method for building `debugString` and `sql`.
   */
  protected def template(childList: Seq[String]): String =
    childList mkString (s"${nodeName.toUpperCase}(", ", ", ")")

  def debugString: String = template(children.map(_.debugString))

  def sql: Try[String] = trySequence(children map (_.sql)) map template

  override def toString: String = debugString
}

trait StatefulExpression[State] extends Expression {
  private var state: Option[State] = None

  override def isPure: Boolean = false

  override def isFoldable: Boolean = false

  override protected def makeCopy(args: Seq[AnyRef]): Expression = {
    state.foreach(throw new BrokenContractException(
      "Stateful expression cannot be copied after initialization"
    ))
    super.makeCopy(args)
  }

  protected def initialState: State

  protected def statefulEvaluate(state: State, input: Row): (Any, State)

  override def evaluate(input: Row): Any = {
    if (state.isEmpty) {
      state = Some(initialState)
    }

    val (result, newState) = statefulEvaluate(state.get, input)
    state = Some(newState)

    result
  }
}

trait NonSQLExpression extends Expression {
  override def sql: Try[String] = Failure(new UnsupportedOperationException(
    s"Expression $debugString doesn't have a SQL representation"
  ))
}

trait LeafExpression extends Expression {
  override def children: Seq[Expression] = Seq.empty

  override def nodeCaption: String = debugString

  override protected def template(childList: Seq[String]): String = template

  protected def template: String = super.template(Nil)
}

trait UnaryExpression extends Expression {
  def child: Expression

  override def children: Seq[Expression] = Seq(child)

  protected def nullSafeEvaluate(value: Any): Any =
    throw new BrokenContractException(
      s"${getClass.getName} must override either evaluate or nullSafeEvaluate"
    )

  override def evaluate(input: Row): Any = {
    Option(child.evaluate(input)).map(nullSafeEvaluate).orNull
  }

  override protected def template(childList: Seq[String]): String = template(childList.head)

  protected def template(childString: String): String = super.template(childString :: Nil)
}

trait BinaryExpression extends Expression {
  def left: Expression

  def right: Expression

  override def children: Seq[Expression] = Seq(left, right)

  def nullSafeEvaluate(lhs: Any, rhs: Any): Any =
    throw new BrokenContractException(
      s"${getClass.getName} must override either evaluate or nullSafeEvaluate"
    )

  override def evaluate(input: Row): Any = {
    val maybeResult = for {
      lhs <- Option(left.evaluate(input))
      rhs <- Option(right.evaluate(input))
    } yield nullSafeEvaluate(lhs, rhs)

    maybeResult.orNull
  }
}

object BinaryExpression {
  def unapply(e: BinaryExpression): Option[(Expression, Expression)] = Some((e.left, e.right))
}

trait Operator { this: Expression =>
  def operator: String
}

trait BinaryOperator extends BinaryExpression with Operator {
  override protected def template(childrenList: Seq[String]): String =
    childrenList mkString ("(", s" $operator ", ")")
}

object BinaryOperator {
  def unapply(e: BinaryOperator): Option[(Expression, Expression)] = Some((e.left, e.right))
}

trait UnaryOperator extends UnaryExpression with Operator {
  override def isNullable: Boolean = child.isNullable

  override protected def template(childString: String): String = s"($operator$childString)"
}

trait UnevaluableExpression extends Expression {
  override def isFoldable: Boolean = false

  override def evaluate(input: Row): Any = throw new ExpressionUnevaluableException(this)
}

trait UnresolvedExpression extends Expression with UnevaluableExpression with NonSQLExpression {
  override def dataType: DataType = throw new ExpressionUnresolvedException(this)

  override def isNullable: Boolean = throw new ExpressionUnresolvedException(this)

  override def strictlyTyped: Try[Expression] = Failure(new ExpressionUnresolvedException(this))

  override def isResolved: Boolean = false

  override def sql: Try[String] = Failure(new UnsupportedOperationException(
    s"Unresolved expression $debugString doesn't have a SQL representation"
  ))
}

case class UnresolvedFunction(name: String, args: Seq[Expression]) extends UnresolvedExpression {
  override def children: Seq[Expression] = args

  override def debugString: String = s"$nodeName($name, ${args map (_.debugString) mkString ", "})"
}
