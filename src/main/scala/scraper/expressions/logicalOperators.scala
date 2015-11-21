package scraper.expressions

import scraper.Row
import scraper.expressions.Cast.implicitlyConvertible
import scraper.types.BooleanType

trait BinaryLogicalPredicate extends Predicate with BinaryExpression {
  override def typeChecked: Boolean =
    childrenTypeChecked && childrenTypes.forall(implicitlyConvertible(_, BooleanType))

  override protected def casted: this.type = (left.dataType, right.dataType) match {
    case (BooleanType, BooleanType) => this
    case (t, BooleanType)           => makeCopy(Cast(left, BooleanType) :: right :: Nil)
    case (BooleanType, t)           => makeCopy(left :: Cast(right, BooleanType) :: Nil)
  }
}

case class And(left: Predicate, right: Predicate) extends BinaryLogicalPredicate {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = {
    lhs.asInstanceOf[Boolean] && rhs.asInstanceOf[Boolean]
  }

  override def caption: String = s"(${left.caption} AND ${right.caption})"
}

case class Or(left: Predicate, right: Predicate) extends BinaryLogicalPredicate {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = {
    lhs.asInstanceOf[Boolean] || rhs.asInstanceOf[Boolean]
  }

  override def caption: String = s"(${left.caption} OR ${right.caption})"
}

case class Not(child: Predicate) extends UnaryPredicate {
  override def evaluate(input: Row): Any = !child.evaluate(input).asInstanceOf[Boolean]

  override def caption: String = s"(NOT ${child.caption})"

  override def typeChecked: Boolean =
    child.typeChecked && implicitlyConvertible(child.dataType, BooleanType)

  override protected def casted: this.type = child.dataType match {
    case BooleanType                                => this
    case t if implicitlyConvertible(t, BooleanType) => makeCopy(Cast(child, BooleanType) :: Nil)
  }
}
