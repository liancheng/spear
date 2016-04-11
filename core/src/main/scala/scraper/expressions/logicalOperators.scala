package scraper.expressions

import scraper.Row
import scraper.expressions.typecheck.{AllCompatible, Exact, TypeConstraints}
import scraper.types.{BooleanType, DataType}

trait BinaryLogicalPredicate extends BinaryOperator {
  override def dataType: DataType = BooleanType

  override protected def typeConstraints: TypeConstraints = Exact(BooleanType, children)
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
  override def dataType: DataType = BooleanType

  override protected def typeConstraints: TypeConstraints = Exact(BooleanType, children)

  override def nullSafeEvaluate(value: Any): Any = !value.asInstanceOf[Boolean]

  override def operator: String = "NOT"

  override protected def template(childString: String): String = s"($operator $childString)"
}

case class If(condition: Expression, yes: Expression, no: Expression) extends Expression {
  override protected def strictDataType: DataType = yes.dataType

  override def children: Seq[Expression] = Seq(condition, yes, no)

  override protected def typeConstraints: TypeConstraints =
    Exact(BooleanType, condition :: Nil) ~ AllCompatible(yes :: no :: Nil)

  override def evaluate(input: Row): Any = condition.evaluate(input) match {
    case null  => null
    case true  => yes evaluate input
    case false => no evaluate input
  }
}
