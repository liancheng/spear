package spear.expressions

import java.lang.{Boolean => JBoolean}

import spear.Row
import spear.expressions.typecheck.TypeConstraint
import spear.types.{BooleanType, DataType}

trait BinaryLogicalPredicate extends BinaryOperator {
  override lazy val dataType: DataType = BooleanType

  override protected lazy val typeConstraint: TypeConstraint = children sameTypeAs BooleanType
}

case class And(left: Expression, right: Expression) extends BinaryLogicalPredicate {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = {
    lhs.asInstanceOf[Boolean] && rhs.asInstanceOf[Boolean]
  }

  override def operator: String = "AND"
}

case class Or(left: Expression, right: Expression) extends BinaryLogicalPredicate {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any =
    lhs.asInstanceOf[Boolean] || rhs.asInstanceOf[Boolean]

  override def operator: String = "OR"
}

case class Not(child: Expression) extends UnaryOperator {
  override lazy val dataType: DataType = BooleanType

  override protected lazy val typeConstraint: TypeConstraint = children sameTypeAs BooleanType

  override def nullSafeEvaluate(value: Any): Any = !value.asInstanceOf[Boolean]

  override def operator: String = "NOT"

  override protected def template(childString: String): String = s"($operator $childString)"
}

case class If(test: Expression, yes: Expression, no: Expression) extends Expression {
  override protected lazy val strictDataType: DataType = yes.dataType

  override def children: Seq[Expression] = Seq(test, yes, no)

  override protected lazy val typeConstraint: TypeConstraint =
    test sameTypeAs BooleanType concat Seq(yes, no).sameType

  override def evaluate(input: Row): Any = test.evaluate(input) match {
    case null  => null
    case true  => yes evaluate input
    case false => no evaluate input
  }
}

case class CaseWhen(
  conditions: Seq[Expression],
  consequences: Seq[Expression],
  alternative: Option[Expression]
) extends Expression {

  require(conditions.nonEmpty && conditions.length == consequences.length)

  override def nodeName: String = "CASE"

  override def children: Seq[Expression] = conditions ++ consequences ++ alternative

  override def evaluate(input: Row): Any = {
    val branches = conditions.iterator map { _ evaluate input } zip consequences.iterator
    def alternativeValue = alternative map { _ evaluate input }

    val hitBranchValue = branches collectFirst {
      case (JBoolean.TRUE, consequence) => consequence evaluate input
    }

    (hitBranchValue orElse alternativeValue).orNull
  }

  def when(condition: Expression, consequence: Expression): CaseWhen = copy(
    conditions = conditions :+ condition,
    consequences = consequences :+ consequence
  )

  def otherwise(expression: Expression): CaseWhen = copy(alternative = Some(expression))

  override protected lazy val typeConstraint: TypeConstraint =
    conditions sameTypeAs BooleanType concat (consequences ++ alternative).sameType

  override protected lazy val strictDataType: DataType = consequences.head.dataType

  override protected def template(childList: Seq[String]): String = {
    val (tests, rest) = childList splitAt conditions.length
    val (values, alternativeString) = rest splitAt conditions.length
    val elsePart = alternativeString.headOption map { " ELSE " + _ } getOrElse ""
    val cases = (tests, values).zipped map { "WHEN " + _ + " THEN " + _ } mkString " "
    s"CASE $cases$elsePart END"
  }
}

object CaseKeyWhen {
  def apply(
    key: Expression,
    candidates: Seq[Expression],
    consequences: Seq[Expression],
    alternative: Option[Expression]
  ): CaseWhen = CaseWhen(candidates map { key === _ }, consequences, alternative)
}
