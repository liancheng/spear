package scraper.expressions

import scraper.Row
import scraper.expressions.typecheck.{AllCompatible, TypeConstraints}
import scraper.types.{BooleanType, DataType}

case class Coalesce(children: Seq[Expression]) extends Expression {
  override protected def strictDataType: DataType = children.head.dataType

  override protected def typeConstraints: TypeConstraints = AllCompatible(children)

  override def evaluate(input: Row): Any =
    (children.iterator map (_ evaluate input) find (_ != null)).orNull
}

object Coalesce {
  def apply(first: Expression, second: Expression, rest: Expression*): Coalesce =
    Coalesce(Seq(first, second) ++ rest)
}

case class IsNull(child: Expression) extends UnaryExpression {
  override def evaluate(input: Row): Any = (child evaluate input) == null

  override def dataType: DataType = BooleanType

  override protected def template(childString: String): String = s"($childString IS NULL)"
}

case class IsNotNull(child: Expression) extends UnaryExpression {
  override def evaluate(input: Row): Any = (child evaluate input) != null

  override def dataType: DataType = BooleanType

  override protected def template(childString: String): String = s"($childString IS NOT NULL)"
}
