package scraper

import scraper.expressions.Expression
import scraper.plans.logical.LogicalPlan
import scraper.types.DataType

case class ParsingException(message: String) extends RuntimeException(message)

abstract class AnalysisException(
  message: String,
  maybeCause: Option[Throwable] = None
) extends RuntimeException(message, maybeCause.orNull)

case class ExpressionUnevaluableException(
  expression: Expression,
  maybeCause: Option[Throwable] = None
) extends {
  val message = s"Expression ${expression.nodeCaption} is unevaluable"
} with AnalysisException(message, maybeCause)

case class ExpressionUnresolvedException(
  expression: Expression,
  maybeCause: Option[Throwable] = None
) extends {
  val message = s"Expression ${expression.nodeCaption} is unresolved"
} with AnalysisException(message, maybeCause)

case class LogicalPlanUnresolved(
  plan: LogicalPlan,
  maybeCause: Option[Throwable] = None
) extends {
  val message = s"Unresolved logical query plan:\n\n${plan.prettyTree}"
} with AnalysisException(message, maybeCause)

case class TypeCheckException(
  message: String,
  maybeCause: Option[Throwable] = None
) extends AnalysisException(message, maybeCause)

object TypeCheckException {
  def apply(expression: Expression, maybeCause: Option[Throwable]): TypeCheckException =
    TypeCheckException({
      s"""Expression ${expression.nodeCaption} doesn't pass type check:
         |
         |${expression.prettyTree}
         |""".stripMargin
    }, maybeCause)

  def apply(plan: LogicalPlan, maybeCause: Option[Throwable]): TypeCheckException =
    TypeCheckException({
      s"""Logical query plan doesn't pass type check:
         |
         |${plan.prettyTree}
         |""".stripMargin
    }, maybeCause)
}

case class TypeCastException(
  from: DataType,
  to: DataType,
  maybeCause: Option[Throwable] = None
) extends AnalysisException(s"Cannot convert data type $from to $to", maybeCause)

case class TypeMismatchException(message: String, maybeCause: Option[Throwable])
  extends AnalysisException(message, maybeCause)

object TypeMismatchException {
  def apply(
    expression: Expression,
    dataTypeClass: Class[_],
    maybeCause: Option[Throwable]
  ): TypeMismatchException = {
    TypeMismatchException({
      val dataType = expression.dataType
      val className = dataTypeClass.getSimpleName.stripSuffix("$")
      s"Expecting $className while expression ${expression.nodeCaption} " +
        s"has type ${dataType.simpleName}."
    }, maybeCause)
  }
}

case class ResolutionFailureException(
  message: String,
  maybeCause: Option[Throwable] = None
) extends AnalysisException(message, maybeCause)
