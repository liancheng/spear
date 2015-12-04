package scraper.expressions

import scala.util.{Success, Try}

import scraper.Row
import scraper.exceptions.{ExpressionUnevaluableException, ExpressionUnresolvedException, TypeCheckException}
import scraper.expressions.dsl.ExpressionDSL
import scraper.trees.TreeNode
import scraper.types.DataType

trait Expression extends TreeNode[Expression] with ExpressionDSL {
  def foldable: Boolean = children.forall(_.foldable)

  def nullable: Boolean = true

  def resolved: Boolean = childrenResolved

  def childrenResolved: Boolean = children forall (_.resolved)

  def references: Seq[Attribute] = children.flatMap(_.references)

  /**
   * Tries to return a strictly typed copy of this [[Expression]].  If this [[Expression]] cannot
   * be converted to strictly typed form, we say it doesn't type check.
   *
   * Any legal [[Expression]] `e` must be either strictly typed or well typed:
   *
   *  1. Strictly typed: `e` is strictly typed iff
   *
   *     - all of its child expressions strictly typed, and
   *     - all of its child expressions immediately meet all type requirements of `e`.
   *
   *  2. Well typed: `e` is well typed iff
   *
   *     - all of its child expressions are well typed, and
   *     - all of its child expressions can meet all type requirements of `e` by applying implicit
   *       cast(s).
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
  def strictlyTypedForm: Try[Expression]

  /**
   * Indicates whether this [[Expression]] is strictly typed.
   *
   * @see [[strictlyTypedForm]]
   */
  lazy val strictlyTyped: Boolean = resolved && (strictlyTypedForm.get sameOrEqual this)

  /**
   * Returns `value` if this [[Expression]] is strictly typed, otherwise throws a
   * [[TypeCheckException]].
   */
  @throws[TypeCheckException]("If this expression is not strictly typed")
  protected def whenStrictlyTyped[T](value: => T): T =
    if (strictlyTyped) value else throw new TypeCheckException(this)

  def dataType: DataType

  def evaluate(input: Row): Any

  def evaluated: Any = evaluate(null)

  def childrenTypes: Seq[DataType] = children.map(_.dataType)

  def annotatedString: String

  def sql: String

  override def nodeCaption: String = getClass.getSimpleName
}

trait LeafExpression extends Expression {
  override def children: Seq[Expression] = Seq.empty

  override lazy val strictlyTypedForm: Try[this.type] = Success(this)

  override def nodeCaption: String = annotatedString
}

trait UnaryExpression extends Expression {
  def child: Expression

  override def children: Seq[Expression] = Seq(child)
}

trait BinaryExpression extends Expression {
  def left: Expression

  def right: Expression

  override def children: Seq[Expression] = Seq(left, right)

  def nullSafeEvaluate(lhs: Any, rhs: Any): Any

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

trait UnevaluableExpression extends Expression {
  override def evaluate(input: Row): Any = throw new ExpressionUnevaluableException(this)
}

trait UnresolvedExpression extends Expression with UnevaluableExpression {
  override def dataType: DataType = throw new ExpressionUnresolvedException(this)

  override def resolved: Boolean = false
}
