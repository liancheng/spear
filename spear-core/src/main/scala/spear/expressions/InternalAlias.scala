package spear.expressions

import spear._
import spear.expressions.NamedExpression.newExpressionID
import spear.expressions.aggregates.AggregateFunction
import spear.expressions.windows.WindowFunction
import spear.types._

sealed trait InternalAlias extends UnaryExpression with NamedExpression with NonSQLExpression {
  def namespace: String

  override def name: Name = i"" withNamespace namespace

  override def debugString: String =
    s"(${child.debugString} AS ${name.toString}#${expressionID.id})"

  override def dataType: DataType = child.dataType

  override def isNullable: Boolean = child.isNullable

  override lazy val isFoldable: Boolean = false

  override def attr: AttributeRef = name of dataType nullable isNullable withID expressionID
}

object InternalAlias {
  val GroupingKeyNamespace: String = "G"

  val AggregateFunctionNamespace: String = "A"

  val WindowFunctionNamespace: String = "W"

  val SortOrderNamespace: String = "S"

  def buildRewriter(aliases: Seq[InternalAlias]): Map[Expression, Expression] =
    aliases.map { a => a.child -> (a.attr: Expression) }.toMap

  def buildRestorer(aliases: Seq[InternalAlias]): Map[Expression, Expression] =
    aliases.map { a => (a.attr: Expression) -> a.child }.toMap
}

case class GroupingKeyAlias(
  child: Expression, override val expressionID: ExpressionID = newExpressionID()
) extends InternalAlias {

  override val namespace: String = InternalAlias.GroupingKeyNamespace

  override def withID(id: ExpressionID): GroupingKeyAlias = copy(expressionID = id)
}

case class AggregateFunctionAlias(
  child: AggregateFunction, override val expressionID: ExpressionID = newExpressionID()
) extends InternalAlias {

  override val namespace: String = InternalAlias.AggregateFunctionNamespace

  override def withID(id: ExpressionID): AggregateFunctionAlias = copy(expressionID = id)
}

case class WindowFunctionAlias(
  child: WindowFunction, override val expressionID: ExpressionID = newExpressionID()
) extends InternalAlias {

  override val namespace: String = InternalAlias.WindowFunctionNamespace

  override def withID(id: ExpressionID): WindowFunctionAlias = copy(expressionID = id)
}

case class SortOrderAlias(
  child: Expression, alias: Name, override val expressionID: ExpressionID = newExpressionID()
) extends InternalAlias {

  override val namespace: String = InternalAlias.SortOrderNamespace

  override def name: Name = alias withNamespace namespace

  override def withID(id: ExpressionID): SortOrderAlias = copy(expressionID = id)
}
