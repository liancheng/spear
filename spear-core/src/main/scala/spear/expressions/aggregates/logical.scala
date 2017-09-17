package spear.expressions.aggregates

import spear.expressions.{And, Expression, Or}
import spear.expressions.aggregates.FoldLeft.AccumulateFunction
import spear.expressions.typecheck.TypeConstraint
import spear.types.BooleanType

case class BoolAnd(child: Expression) extends NullableReduceLeft {
  override def nodeName: String = "bool_and"

  override val accumulateFunction: AccumulateFunction = And

  override protected lazy val typeConstraint: TypeConstraint = children sameTypeAs BooleanType
}

case class BoolOr(child: Expression) extends NullableReduceLeft {
  override def nodeName: String = "bool_or"

  override val accumulateFunction: AccumulateFunction = Or

  override protected lazy val typeConstraint: TypeConstraint = children sameTypeAs BooleanType
}
