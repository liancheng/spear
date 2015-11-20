package scraper.expressions

import scraper.Row
import scraper.expressions.Cast.implicitlyCastable
import scraper.types.{BooleanType, DataType}

trait Predicate extends Expression {
  override def dataType: DataType = BooleanType

  def &&(that: Predicate): And = And(this, that)

  def and(that: Predicate): And = this && that

  def ||(that: Predicate): Or = Or(this, that)

  def or(that: Predicate): Or = this || that

  def unary_! : Not = Not(this)
}

object Predicate {
  private[scraper] def splitConjunction(predicate: Predicate): Seq[Predicate] = predicate match {
    case left And right => splitConjunction(left) ++ splitConjunction(right)
    case _              => predicate :: Nil
  }

  private[scraper] def splitDisjunction(predicate: Predicate): Seq[Predicate] = predicate match {
    case left Or right => splitDisjunction(left) ++ splitDisjunction(right)
    case _             => predicate :: Nil
  }
}

trait BinaryLogicalPredicate extends Predicate with BinaryExpression {
  override def typeChecked: Boolean =
    childrenTypeChecked && childrenTypes.forall(implicitlyCastable(_, BooleanType))

  override protected def casted: this.type = (left.dataType, right.dataType) match {
    case (BooleanType, BooleanType) => this
    case (t, BooleanType)           => makeCopy(Cast(left, BooleanType) :: right :: Nil)
    case (BooleanType, t)           => makeCopy(left :: Cast(right, BooleanType) :: Nil)
  }
}

trait UnaryPredicate extends Predicate with UnaryExpression

trait LeafPredicate extends Predicate with LeafExpression

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
    child.typeChecked && implicitlyCastable(child.dataType, BooleanType)

  override protected def casted: this.type = child.dataType match {
    case BooleanType                             => this
    case t if implicitlyCastable(t, BooleanType) => makeCopy(Cast(child, BooleanType) :: Nil)
  }
}

case class EqualTo(left: Expression, right: Expression) extends Predicate with BinaryExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = lhs == rhs

  override def caption: String = s"(${left.caption} = ${right.caption})"

  override def typeChecked: Boolean = childrenTypeChecked && (
    implicitlyCastable(left.dataType, right.dataType) ||
    implicitlyCastable(right.dataType, left.dataType)
  )

  override protected def casted: this.type = (left.dataType, right.dataType) match {
    case (lhsType, rhsType) if lhsType == rhsType =>
      this

    case (lhsType, rhsType) if implicitlyCastable(lhsType, rhsType) =>
      makeCopy(Cast(left, rhsType) :: right :: Nil)

    case (lhsType, rhsType) if implicitlyCastable(rhsType, lhsType) =>
      makeCopy(left :: Cast(right, lhsType) :: Nil)
  }
}

case class NotEqualTo(left: Expression, right: Expression) extends Predicate with BinaryExpression {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = lhs != rhs

  override def caption: String = s"(${left.caption} != ${right.caption})"

  override def typeChecked: Boolean = childrenTypeChecked && (
    implicitlyCastable(left.dataType, right.dataType) ||
    implicitlyCastable(right.dataType, left.dataType)
  )

  override protected def casted: this.type = (left.dataType, right.dataType) match {
    case (lhsType, rhsType) if lhsType == rhsType =>
      this

    case (lhsType, rhsType) if implicitlyCastable(lhsType, rhsType) =>
      makeCopy(Cast(left, rhsType) :: right :: Nil)

    case (lhsType, rhsType) if implicitlyCastable(rhsType, lhsType) =>
      makeCopy(left :: Cast(right, lhsType) :: Nil)
  }
}
