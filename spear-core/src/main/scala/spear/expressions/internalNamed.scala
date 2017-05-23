package spear.expressions

import spear._
import spear.expressions.InternalNamedExpression._
import spear.expressions.NamedExpression.newExpressionID
import spear.expressions.aggregates.AggregateFunction
import spear.expressions.windows.WindowFunction
import spear.types._

sealed trait InternalNamedExpression extends NamedExpression with NonSQLExpression {
  def purpose: Purpose

  override def name: Name = purpose.name
}

object InternalNamedExpression {
  /**
   * Indicates the purpose of a [[InternalNamedExpression]].
   */
  sealed trait Purpose {
    def namespace: String

    def name: Name = i"" withNamespace namespace
  }

  /**
   * Marks [[InternalNamedExpression]]s that are used to wrap/reference grouping key expressions.
   */
  case object ForGrouping extends Purpose {
    override def namespace: String = "G"
  }

  /**
   * Marks [[InternalNamedExpression]]s that are used to wrap/reference aggregate functions.
   */
  case object ForAggregation extends Purpose {
    override def namespace: String = "A"
  }

  /**
   * Marks [[InternalNamedExpression]]s that are used to wrap/reference window functions.
   */
  case object ForWindow extends Purpose {
    override def namespace: String = "W"
  }

  case object ForSortOrder extends Purpose {
    override def namespace: String = "S"
  }
}

trait InternalAlias extends UnaryExpression with InternalNamedExpression {
  override def debugString: String =
    s"(${child.debugString} AS ${name.toString}#${expressionID.id})"

  override def dataType: DataType = child.dataType

  override def isNullable: Boolean = child.isNullable

  override lazy val isFoldable: Boolean = false

  override def attr: InternalAttribute
}

object InternalAlias {
  def buildRewriter(aliases: Seq[InternalAlias]): Map[Expression, Expression] =
    aliases.map { a => a.child -> (a.attr: Expression) }.toMap

  def buildRestorer(aliases: Seq[InternalAlias]): Map[Expression, Expression] =
    aliases.map { a => (a.attr: Expression) -> a.child }.toMap
}

trait InternalAttribute extends LeafExpression
  with ResolvedAttribute
  with UnevaluableExpression
  with InternalNamedExpression

case class GroupingAlias(
  child: Expression,
  override val expressionID: ExpressionID = newExpressionID()
) extends InternalAlias {
  override val purpose: Purpose = ForGrouping

  override def attr: GroupingAttribute = GroupingAttribute(dataType, isNullable, expressionID)

  override def withID(id: ExpressionID): GroupingAlias = copy(expressionID = id)
}

case class GroupingAttribute(
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID
) extends InternalAttribute {
  override val purpose: Purpose = ForGrouping

  override def withID(id: ExpressionID): Attribute = copy(expressionID = id)
}

case class AggregationAlias(
  child: AggregateFunction,
  override val expressionID: ExpressionID = newExpressionID()
) extends InternalAlias {
  override val purpose: Purpose = ForAggregation

  override def attr: AggregationAttribute = AggregationAttribute(dataType, isNullable, expressionID)

  override def withID(id: ExpressionID): AggregationAlias = copy(expressionID = id)
}

case class AggregationAttribute(
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID
) extends InternalAttribute {
  override val purpose: Purpose = ForAggregation

  override def withID(id: ExpressionID): Attribute = copy(expressionID = id)
}

case class WindowAlias(
  child: WindowFunction,
  override val expressionID: ExpressionID = newExpressionID()
) extends InternalAlias {
  override def purpose: Purpose = ForWindow

  override def attr: InternalAttribute = WindowAttribute(dataType, isNullable, expressionID)

  override def withID(id: ExpressionID): WindowAlias = copy(expressionID = id)
}

case class WindowAttribute(
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID
) extends InternalAttribute {
  override val purpose: Purpose = ForWindow

  override def withID(id: ExpressionID): Attribute = copy(expressionID = id)
}

case class SortOrderAlias(
  child: Expression,
  alias: Name,
  override val expressionID: ExpressionID = newExpressionID()
) extends InternalAlias {
  override def purpose: Purpose = ForSortOrder

  override def name: Name = alias withNamespace purpose.namespace

  override def attr: InternalAttribute =
    SortOrderAttribute(dataType, isNullable, expressionID, alias)

  override def withID(id: ExpressionID): SortOrderAlias = copy(expressionID = id)
}

case class SortOrderAttribute(
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID,
  alias: Name
) extends InternalAttribute {
  override val purpose: Purpose = ForSortOrder

  override def name: Name = alias withNamespace purpose.namespace

  override def withID(id: ExpressionID): Attribute = copy(expressionID = id)
}
