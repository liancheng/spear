package scraper.expressions

import scala.util.Try

import scraper.Row
import scraper.expressions.Cast.{promoteDataType, widestTypeOf}
import scraper.types.{BooleanType, DataType}

case class Coalesce(children: Seq[Expression]) extends Expression {
  override protected def strictDataType: DataType = children.head.dataType

  override lazy val strictlyTypedForm: Try[Coalesce] = for {
    strictChildren <- Try(children map (_.strictlyTypedForm.get))
    finalType <- widestTypeOf(strictChildren map (_.dataType))
    promotedChildren = children.map(promoteDataType(_, finalType))
  } yield if (sameChildren(promotedChildren)) this else copy(children = promotedChildren)

  override def sql: String = s"COALESCE(${children map (_.sql) mkString ", "})"

  override def annotatedString: String =
    s"COALESCE(${children map (_.annotatedString) mkString ", "})"

  override def evaluate(input: Row): Any =
    (children.iterator map (_ evaluate input) find (_ != null)).orNull
}

object Coalesce {
  def apply(first: Expression, second: Expression, rest: Expression*): Coalesce =
    Coalesce(Seq(first, second) ++ rest)
}

case class IsNull(child: Expression) extends UnaryExpression {
  override def strictlyTypedForm: Try[Expression] = for {
    strictChild <- child.strictlyTypedForm
  } yield if (strictChild sameOrEqual child) this else copy(child = strictChild)

  override def sql: String = s"(${child.sql} IS NULL)"

  override def evaluate(input: Row): Any = (child evaluate input) == null

  override def dataType: DataType = BooleanType

  override def annotatedString: String = s"(${child.sql} IS NULL)"
}

case class IsNotNull(child: Expression) extends UnaryExpression {
  override def strictlyTypedForm: Try[Expression] = for {
    strictChild <- child.strictlyTypedForm
  } yield if (strictChild sameOrEqual child) this else copy(child = strictChild)

  override def sql: String = s"(${child.sql} IS NOT NULL)"

  override def evaluate(input: Row): Any = (child evaluate input) != null

  override def dataType: DataType = BooleanType

  override def annotatedString: String = s"(${child.annotatedString} IS NOT NULL)"
}
