package scraper.expressions

import scraper.trees.TreeNode
import scraper.types.DataType
import scraper.{ ExpressionUnevaluable, ExpressionUnresolved, Row }

trait Expression extends TreeNode[Expression] {
  def foldable: Boolean = children.forall(_.foldable)

  def nullable: Boolean = true

  def resolved: Boolean = children.forall(_.resolved)

  def references: Seq[Attribute] = children.flatMap(_.references)

  def deterministic: Boolean = children.forall(_.deterministic)

  def dataType: DataType

  def evaluate(input: Row): Any

  def evaluated: Any = evaluate(null)

  protected def predicate: Predicate = this match { case e: Predicate => e }

  def +(that: Expression): Expression = Add(this, that)

  def -(that: Expression): Expression = Minus(this, that)

  def *(that: Expression): Expression = Multiply(this, that)

  def ===(that: Expression): EqualTo = EqualTo(this, that)

  def <>(that: Expression): NotEqualTo = NotEqualTo(this, that)

  def as(alias: String): Alias = Alias(alias, this)

  def as(alias: Symbol): Alias = Alias(alias.name, this)

  def cast(dataType: DataType): Cast = Cast(this, dataType)
}

trait LeafExpression extends Expression {
  override def children: Seq[Expression] = Seq.empty
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

trait UnevaluableExpression extends Expression {
  override def evaluate(input: Row): Any = throw ExpressionUnevaluable(this)
}

trait UnresolvedExpression extends Expression with UnevaluableExpression {
  override def dataType: DataType = throw ExpressionUnresolved(this)

  override def resolved: Boolean = false
}
