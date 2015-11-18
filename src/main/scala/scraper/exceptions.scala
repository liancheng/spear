package scraper

import scraper.expressions.Expression
import scraper.plans.logical.LogicalPlan
import scraper.types.DataType

case class ExpressionUnevaluable(expression: Expression) extends RuntimeException

case class ExpressionUnresolved(expression: Expression) extends RuntimeException

case class LogicalPlanUnresolved(plan: LogicalPlan) extends RuntimeException

case class ParsingError(message: String) extends RuntimeException(message)

case class TypeCastError(from: DataType, to: DataType)
  extends RuntimeException(s"Cannot convert data type $from to $to")

case class TypeCheckError(expression: Expression)
  extends RuntimeException(s"Expression $expression doesn't type check")

case class ResolutionFailure(message: String) extends RuntimeException(message)
