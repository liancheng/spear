package scraper.expressions.aggregates

import scraper.Name
import scraper.expressions.{And, Expression, Or}
import scraper.expressions.aggregates.FoldLeft.UpdateFunction
import scraper.expressions.typecheck.TypeConstraint
import scraper.types.BooleanType

abstract class LogicalNullableReduceLeft extends NullableReduceLeft {
  override protected lazy val typeConstraint: TypeConstraint = children sameTypeAs BooleanType
}

case class BoolAnd(child: Expression) extends LogicalNullableReduceLeft {
  override def nodeName: Name = "bool_and"

  override val updateFunction: UpdateFunction = And
}

case class BoolOr(child: Expression) extends LogicalNullableReduceLeft {
  override def nodeName: Name = "bool_or"

  override val updateFunction: UpdateFunction = Or
}
