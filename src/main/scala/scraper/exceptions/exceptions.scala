package scraper.exceptions

import scraper.expressions.Expression
import scraper.plans.logical.LogicalPlan
import scraper.types.DataType

class ParsingException(message: String) extends RuntimeException(message)

abstract class AnalysisException(
  message: String,
  maybeCause: Option[Throwable] = None
) extends RuntimeException(message, maybeCause.orNull)

class ExpressionUnevaluableException(
  expression: Expression,
  maybeCause: Option[Throwable] = None
) extends {
  val message = s"Expression ${expression.annotatedString} is unevaluable"
} with AnalysisException(message, maybeCause)

class ExpressionUnresolvedException(
  expression: Expression,
  maybeCause: Option[Throwable] = None
) extends {
  val message = s"Expression ${expression.nodeCaption} is unresolved"
} with AnalysisException(message, maybeCause)

class LogicalPlanUnresolved(
  plan: LogicalPlan,
  maybeCause: Option[Throwable] = None
) extends {
  val message = s"Unresolved logical query plan:\n\n${plan.prettyTree}"
} with AnalysisException(message, maybeCause)

class TypeCheckException(
  message: String,
  maybeCause: Option[Throwable]
) extends AnalysisException(message, maybeCause) {
  def this(expression: Expression, maybeCause: Option[Throwable]) =
    this({
      s"""Expression ${expression.nodeCaption} doesn't pass type check:
         |
         |${expression.prettyTree}
         |""".stripMargin
    }, maybeCause)

  def this(plan: LogicalPlan, maybeCause: Option[Throwable]) =
    this({
      s"""Logical query plan doesn't pass type check:
         |
         |${plan.prettyTree}
         |""".stripMargin
    }, maybeCause)
}

class TypeCastException(
  message: String, maybeCause: Option[Throwable]
) extends AnalysisException(message, maybeCause) {
  def this(from: DataType, to: DataType, maybeCause: Option[Throwable] = None) =
    this(s"Cannot convert data type $from to $to", maybeCause)
}

class ImplicitCastException(from: DataType, to: DataType, maybeCause: Option[Throwable] = None)
  extends TypeCastException(s"Cannot convert data type $from to $to implicitly", maybeCause)

class TypeMismatchException(message: String, maybeCause: Option[Throwable])
  extends AnalysisException(message, maybeCause) {

  def this(
    expression: Expression, dataTypeClass: Class[_], maybeCause: Option[Throwable] = None
  ) = this({
    val dataType = expression.dataType
    val expected = dataTypeClass.getSimpleName.stripSuffix("$")
    val actual = dataType.getClass.getSimpleName.stripSuffix("$")
    s"Expecting $expected while expression ${expression.annotatedString} has type $actual, " +
      s"which cannot be implicitly casted to $expected."
  }, maybeCause)
}

class ResolutionFailureException(
  message: String,
  maybeCause: Option[Throwable] = None
) extends AnalysisException(message, maybeCause)
