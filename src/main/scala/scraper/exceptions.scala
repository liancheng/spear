package scraper

import scraper.expressions.{ UnresolvedExpression, UnevaluableExpression }
import scraper.plans.logical.LogicalPlan
import scraper.types.DataType

case class ExpressionUnevaluable(expression: UnevaluableExpression) extends RuntimeException

case class ExpressionUnresolved(expression: UnresolvedExpression) extends RuntimeException

case class LogicalPlanUnresolved(plan: LogicalPlan) extends RuntimeException

case class ParsingError(message: String) extends RuntimeException(message)

case class TypeCastError(from: DataType, to: DataType)
  extends RuntimeException(s"Cannot convert data type $from to $to")

case class ResolutionFailure(message: String) extends RuntimeException(message)
