package scraper.expressions.aggregates

import scraper.expressions.{And, Expression, Or}
import scraper.expressions.aggregates.FoldLeft.UpdateFunction
import scraper.expressions.typecheck.TypeConstraint
import scraper.types.BooleanType

case class BoolAnd(child: Expression) extends NullableReduceLeft {
  override def nodeName: String = "bool_and"

  override val updateFunction: UpdateFunction = And

  override protected lazy val typeConstraint: TypeConstraint = children sameTypeAs BooleanType
}

case class BoolOr(child: Expression) extends NullableReduceLeft {
  override def nodeName: String = "bool_or"

  override val updateFunction: UpdateFunction = Or

  override protected lazy val typeConstraint: TypeConstraint = children sameTypeAs BooleanType
}
