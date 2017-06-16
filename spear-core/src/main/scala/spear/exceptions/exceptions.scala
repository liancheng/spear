package spear.exceptions

import spear.Name
import spear.expressions.Expression
import spear.plans.logical.LogicalPlan
import spear.types.{AbstractDataType, DataType}
import spear.utils._

class ParsingException(message: String) extends RuntimeException(message)

class ContractBrokenException(message: String, cause: Throwable)
  extends RuntimeException(message, cause) {

  def this(message: String) = this(message, null)
}

class ExpressionUnevaluableException(expression: Expression, cause: Throwable)
  extends RuntimeException(s"Expression ${expression.sqlLike} is unevaluable", cause) {

  def this(expression: Expression) = this(expression, null)
}

class AnalysisException(message: String, cause: Throwable)
  extends RuntimeException(message, cause) {

  def this(message: String) = this(message, null)
}

class ExpressionUnresolvedException(expression: Expression, cause: Throwable)
  extends AnalysisException(s"Expression ${expression.caption} is unresolved", cause) {

  def this(expression: Expression) = this(expression, null)
}

class NameUnresolvedException(expression: Expression, cause: Throwable)
  extends AnalysisException(
    s"The name of expression ${expression.caption} is unresolved", cause
  ) {

  def this(expression: Expression) = this(expression, null)
}

class ExpressionNotBoundException(expression: Expression, cause: Throwable)
  extends AnalysisException(s"Expression ${expression.caption} is not bound", cause) {

  def this(expression: Expression) = this(expression, null)
}

class LogicalPlanUnresolvedException(plan: LogicalPlan, cause: Throwable)
  extends AnalysisException(s"Unresolved logical query plan:\n\n${plan.prettyTree}", cause) {

  def this(plan: LogicalPlan) = this(plan, null)
}

class TypeCheckException(message: String, cause: Throwable)
  extends AnalysisException(message, cause) {

  def this(expression: Expression, cause: Throwable) =
    this({
      val causeMessage = Option(cause) map { ". " + _.getMessage } getOrElse ""
      s"""Expression ${expression.sqlLike} doesn't pass type check$causeMessage
         |${expression.prettyTree}
         |""".stripMargin
    }, cause)

  def this(expression: Expression) = this(expression, null)

  def this(plan: LogicalPlan, cause: Throwable) =
    this(
      s"""Logical query plan doesn't pass type check:
         |${plan.prettyTree}
         |""".stripMargin,
      cause
    )

  def this(plan: LogicalPlan) = this(plan, null)
}

class TypeCastException(message: String, cause: Throwable)
  extends AnalysisException(message, cause) {

  def this(message: String) = this(message, null)

  def this(from: DataType, to: DataType, cause: Throwable) =
    this(s"Cannot convert data type ${from.sql} to ${from.sql}", cause)

  def this(from: DataType, to: DataType) = this(from, to, null)
}

class ImplicitCastException(message: String, cause: Throwable)
  extends TypeCastException(message, cause) {

  def this(from: Expression, to: DataType, cause: Throwable = null) = this({
    s"""Cannot convert expression ${from.sqlLike} of data type ${from.dataType.sql} to ${to.sql}
       |implicitly.
       |""".oneLine
  }, cause)

  def this(from: Expression, to: DataType) = this(from, to, null)
}

class TypeMismatchException(message: String, cause: Throwable)
  extends AnalysisException(message, cause) {

  def this(message: String) = this(message, null)

  def this(
    expressions: Seq[Expression],
    expected: DataType,
    cause: Throwable
  ) = this({
    val violators = expressions map { e =>
      s" - Expression ${e.sqlLike} is of type ${e.dataType.sql}"
    }

    s"""Expecting expression(s) of type ${expected.sql}, but found the following violator(s):
       |${violators mkString "\n"}
       |""".stripMargin
  }, cause)

  def this(
    expressions: Seq[Expression],
    expected: AbstractDataType,
    cause: Throwable
  ) = this({
    val violators = expressions map { e =>
      s" - Expression ${e.sqlLike} is of type ${e.dataType.sql}"
    }

    s"""Expecting expression(s) of ${expected.toString}, but found the following violator(s):
       |${violators mkString "\n"}
       |""".stripMargin
  }, cause)

  def this(expressions: Seq[Expression], expected: DataType) =
    this(expressions, expected, null)

  def this(expressions: Seq[Expression], expected: AbstractDataType) =
    this(expressions, expected, null)
}

class ResolutionFailureException(message: String, cause: Throwable)
  extends AnalysisException(message, cause) {

  def this(message: String) = this(message, null)
}

class TableNotFoundException(tableName: Name, cause: Throwable)
  extends AnalysisException(s"Table $tableName not found", cause) {

  def this(tableName: Name) = this(tableName, null)
}

class FunctionNotFoundException(name: Name, cause: Throwable)
  extends AnalysisException(s"Function $name not found", cause) {

  def this(name: Name) = this(name, null)
}

class FunctionInstantiationException(name: Name, cause: Throwable)
  extends AnalysisException(s"Failed to instantiate function $name", cause) {

  def this(name: Name) = this(name, null)
}

class IllegalAggregationException(message: String, cause: Throwable)
  extends AnalysisException(message, cause) {

  def this(message: String) = this(message, null)
}

class SettingsValidationException(message: String, cause: Throwable)
  extends RuntimeException(message, cause) {

  def this(message: String) = this(message, null)
}

class WindowAnalysisException(message: String, cause: Throwable)
  extends AnalysisException(message, cause) {

  def this(message: String) = this(message, null)
}
