package scraper.expressions

import java.util.regex.Pattern

import scraper.Row
import scraper.expressions.typecheck.TypeConstraint
import scraper.types.{BooleanType, DataType, StringType}

case class Concat(children: Seq[Expression]) extends Expression {
  override def dataType: DataType = StringType

  override protected lazy val typeConstraint: TypeConstraint = children sameTypeAs StringType

  override def evaluate(input: Row): Any =
    (children map (_ evaluate input) map (_.asInstanceOf[String]) filter (_ != null)).mkString
}

case class RLike(string: Expression, pattern: Expression) extends Expression {
  override def dataType: DataType = BooleanType

  override protected lazy val typeConstraint: TypeConstraint =
    (Seq(string) sameTypeAs StringType) ++ (Seq(pattern) sameTypeAs StringType andThen (_.foldable))

  private lazy val compiledPattern = {
    val evaluatedPattern: String = pattern.evaluated.asInstanceOf[String]
    Pattern.compile(evaluatedPattern)
  }

  override def children: Seq[Expression] = string :: pattern :: Nil

  override def evaluate(input: Row): Any = {
    compiledPattern.matcher(string.evaluate(input).asInstanceOf[String]).matches()
  }
}
