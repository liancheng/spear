package scraper.exceptions

import scraper.expressions.{AttributeRef, Expression}
import scraper.plans.logical.LogicalPlan
import scraper.types.DataType
import scraper.utils._

class ParsingException(message: String) extends RuntimeException(message)

class BrokenContractException(message: String, cause: Throwable)
  extends RuntimeException(message, cause) {

  def this(message: String) = this(message, null)
}

abstract class AnalysisException(message: String, cause: Throwable)
  extends RuntimeException(message, cause)

class ExpressionUnevaluableException(expression: Expression, cause: Throwable)
  extends AnalysisException(s"Expression ${expression.debugString} is unevaluable", cause) {

  def this(expression: Expression) = this(expression, null)
}

class ExpressionUnresolvedException(expression: Expression, cause: Throwable)
  extends AnalysisException(s"Expression ${expression.nodeCaption} is unresolved", cause) {

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
      s"""Expression [${expression.debugString}] doesn't pass type check:
         |${expression.prettyTree}
         |""".stripMargin
    }, cause)

  def this(expression: Expression) = this(expression, null)

  def this(plan: LogicalPlan, cause: Throwable) =
    this({
      s"""Logical query plan doesn't pass type check:
         |${plan.prettyTree}
         |""".stripMargin
    }, cause)

  def this(plan: LogicalPlan) = this(plan, null)
}

class TypeCastException(message: String, cause: Throwable)
  extends AnalysisException(message, cause) {

  def this(from: DataType, to: DataType, cause: Throwable) =
    this(s"Cannot convert data type $from to $to", cause)

  def this(from: DataType, to: DataType) = this(from, to, null)
}

class ImplicitCastException(message: String, cause: Throwable)
  extends TypeCastException(message, cause) {

  def this(from: Expression, to: DataType, cause: Throwable) = this({
    s"""Cannot convert expression [${from.debugString}]
       |of data type ${from.dataType} to $to implicitly.
     """.straight
  }, cause)

  def this(from: Expression, to: DataType) = this(from, to, null)
}

class TypeMismatchException(message: String, cause: Throwable)
  extends AnalysisException(message, cause) {

  def this(message: String) = this(message, null)

  def this(expression: Expression, dataTypeClass: Class[_], cause: Throwable) = this({
    val expected = dataTypeClass.getSimpleName stripSuffix "$"
    val actual = expression.dataType.getClass.getSimpleName stripSuffix "$"
    s"""Expression [${expression.debugString}] has type $actual,
       |which cannot be implicitly converted to expected type $expected.
     """.straight
  }, cause)

  def this(expression: Expression, dataTypeClass: Class[_]) =
    this(expression, dataTypeClass, null)

  def this(expression: Expression, dataType: DataType) =
    this(expression, dataType.getClass, null)
}

class ResolutionFailureException(message: String, cause: Throwable)
  extends AnalysisException(message, cause) {

  def this(message: String) = this(message, null)
}

class TableNotFoundException(tableName: String, cause: Throwable)
  extends AnalysisException(s"Table $tableName not found", cause) {

  def this(tableName: String) = this(tableName, null)
}

class FunctionNotFoundException(name: String, cause: Throwable)
  extends AnalysisException(s"Function $name not found", cause) {

  def this(name: String) = this(name, null)
}

class SchemaIncompatibleException(message: String, cause: Throwable)
  extends AnalysisException(message, cause) {

  def this(message: String) = this(message, null)
}

class IllegalAggregationException(message: String, cause: Throwable)
  extends AnalysisException(message, cause) {

  def this(
    part: String,
    attribute: AttributeRef,
    expression: Expression,
    keys: Seq[Expression],
    cause: Throwable
  ) = this({
    val e = expression.sql.get
    val a = attribute.sql.get
    val ks = keys map (_.sql.get) mkString ("[", ", ", "]")
    s"$part $e references $a, which is neither an aggregate function nor a grouping key among $ks"
  }, cause)

  def this(part: String, attribute: AttributeRef, expression: Expression, keys: Seq[Expression]) =
    this(part, attribute, expression, keys, null)
}

class SettingsValidationException(message: String, cause: Throwable)
  extends RuntimeException(message, cause) {

  def this(message: String) = this(message, null)
}
