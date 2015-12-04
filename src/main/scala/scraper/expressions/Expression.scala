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

  def strictlyTypedForm: Try[Expression]

  def dataType: DataType

  def evaluate(input: Row): Any

  def evaluated: Any = evaluate(null)

  def childrenTypes: Seq[DataType] = children.map(_.dataType)

  lazy val strictlyTyped: Boolean = resolved && (strictlyTypedForm.get sameOrEqual this)

  protected def whenStrictlyTyped[T](value: => T): T =
    if (strictlyTyped) value else throw new TypeCheckException(this)

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

  protected def nullSafeEvaluate(value: Any): Any =
    sys.error(s"UnaryExpressions must override either eval or nullSafeEval")

  override def evaluate(input: Row): Any = {
    Option(child.evaluate(input)).map(nullSafeEvaluate).orNull
  }
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
