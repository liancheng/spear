package scraper.exceptions

import scraper.expressions.Expression
import scraper.plans.logical.LogicalPlan
import scraper.types.DataType
import scraper.utils._

class ParsingException(message: String) extends RuntimeException(message)

class ContractBrokenException protected (message: String, maybeCause: Option[Throwable])
  extends RuntimeException(message, maybeCause.orNull) {

  def this(message: String) = this(message, None)

  def this(message: String, cause: Throwable) = this(message, Some(cause))
}

abstract class AnalysisException protected (message: String, maybeCause: Option[Throwable])
  extends RuntimeException(message, maybeCause.orNull) {

  def this(message: String, cause: Throwable) = this(message, Some(cause))
}

class ExpressionUnevaluableException protected (
  expression: Expression, maybeCause: Option[Throwable]
) extends AnalysisException(
  s"Expression ${expression.annotatedString} is unevaluable", maybeCause
) {
  def this(expression: Expression) = this(expression, None)

  def this(expression: Expression, cause: Throwable) = this(expression, Some(cause))
}

class ExpressionUnresolvedException protected (
  expression: Expression, maybeCause: Option[Throwable]
) extends AnalysisException(s"Expression ${expression.nodeCaption} is unresolved", maybeCause) {
  def this(expression: Expression) = this(expression, None)

  def this(expression: Expression, cause: Throwable) = this(expression, Some(cause))
}

class LogicalPlanUnresolved protected (plan: LogicalPlan, maybeCause: Option[Throwable])
  extends AnalysisException(s"Unresolved logical query plan:\n\n${plan.prettyTree}", maybeCause) {

  def this(plan: LogicalPlan) = this(plan, None)

  def this(plan: LogicalPlan, cause: Throwable) = this(plan, Some(cause))
}

class TypeCheckException protected (message: String, maybeCause: Option[Throwable])
  extends AnalysisException(message, maybeCause) {

  def this(expression: Expression, maybeCause: Option[Throwable]) =
    this({
      s"""Expression [${expression.annotatedString}] doesn't pass type check:
         |
         |${expression.prettyTree}
         |""".stripMargin
    }, maybeCause)

  def this(expression: Expression, cause: Throwable) = this(expression, Some(cause))

  def this(expression: Expression) = this(expression, None)

  def this(plan: LogicalPlan, maybeCause: Option[Throwable]) =
    this({
      s"""Logical query plan doesn't pass type check:
         |
         |${plan.prettyTree}
         |""".stripMargin
    }, maybeCause)

  def this(plan: LogicalPlan, cause: Throwable) = this(plan, Some(cause))

  def this(plan: LogicalPlan) = this(plan, None)
}

class TypeCastException protected (message: String, maybeCause: Option[Throwable])
  extends AnalysisException(message, maybeCause) {

  def this(from: DataType, to: DataType, maybeCause: Option[Throwable]) =
    this(s"Cannot convert data type $from to $to", maybeCause)

  def this(from: DataType, to: DataType, cause: Throwable) = this(from, to, Some(cause))

  def this(from: DataType, to: DataType) = this(from, to, None)
}

class ImplicitCastException protected (message: String, maybeCause: Option[Throwable])
  extends TypeCastException(message, maybeCause) {

  def this(from: Expression, to: DataType, maybeCause: Option[Throwable]) = this({
    s"""Cannot convert expression [${from.annotatedString}]
       |of data type ${from.dataType} to $to implicitly.
     """.straight
  }, maybeCause)

  def this(from: Expression, to: DataType, cause: Throwable) =
    this(from, to, Some(cause))

  def this(from: Expression, to: DataType) =
    this(from, to, None)
}

class TypeMismatchException protected (message: String, maybeCause: Option[Throwable])
  extends AnalysisException(message, maybeCause) {

  def this(message: String) = this(message, None)

  def this(message: String, cause: Throwable) = this(message, Some(cause))

  protected def this(
    expression: Expression, dataTypeClass: Class[_], maybeCause: Option[Throwable]
  ) = this({
    val expected = dataTypeClass.getSimpleName stripSuffix "$"
    val actual = expression.dataType.getClass.getSimpleName stripSuffix "$"
    s"""Expression [${expression.annotatedString}] has type $actual,
        |which cannot be implicitly converted to expected type $expected.
     """.straight
  }, maybeCause)

  def this(expression: Expression, dataTypeClass: Class[_], cause: Throwable) =
    this(expression, dataTypeClass, Some(cause))

  def this(expression: Expression, dataTypeClass: Class[_]) =
    this(expression, dataTypeClass, None)

  def this(expression: Expression, dataType: DataType, cause: Throwable) =
    this(expression, dataType.getClass, Some(cause))

  def this(expression: Expression, dataType: DataType) =
    this(expression, dataType.getClass, None)
}

class ResolutionFailureException protected (message: String, maybeCause: Option[Throwable])
  extends AnalysisException(message, maybeCause) {

  def this(message: String) = this(message, None)

  def this(message: String, cause: Throwable) = this(message, Some(cause))
}

class TableNotFoundException protected (tableName: String, maybeCause: Option[Throwable])
  extends AnalysisException(s"Table $tableName not found", maybeCause) {

  def this(tableName: String) = this(tableName, None)

  def this(tableName: String, cause: Throwable) = this(tableName, Some(cause))
}

class SettingsValidationException protected (message: String, maybeCause: Option[Throwable])
  extends RuntimeException(message, maybeCause.orNull) {

  def this(message: String) = this(message, None)

  def this(message: String, cause: Throwable) = this(message, Some(cause))
}
