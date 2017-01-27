package scraper.expressions

import java.util.regex.Pattern

import scraper.Row
import scraper.expressions.typecheck.{Foldable, TypeConstraint}
import scraper.types.{BooleanType, DataType, StringType}

case class Concat(children: Seq[Expression]) extends Expression {
  override def dataType: DataType = StringType

  override protected def typeConstraint: TypeConstraint = children sameTypeAs StringType

  override def evaluate(input: Row): Any =
    (children map (_ evaluate input) map (_.asInstanceOf[String]) filter (_ != null)).mkString
}

case class RLike(left: Expression, right: Expression) extends BinaryOperator {
  override def operator: String = "RLIKE"

  override def dataType: DataType = BooleanType

  override protected def typeConstraint: TypeConstraint =
    left sameTypeAs StringType concat (right sameTypeAs StringType andAlso Foldable)

  private lazy val compiledPattern =
    Pattern.compile(right.evaluated match { case pattern: String => pattern })

  override def evaluate(input: Row): Any =
    compiledPattern.matcher(left.evaluate(input).asInstanceOf[String]).matches()
}
