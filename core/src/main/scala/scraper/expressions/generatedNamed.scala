package scraper.expressions

import scraper.expressions.GeneratedNamedExpression.{ForAggregation, ForGrouping, Purpose}
import scraper.expressions.NamedExpression.newExpressionID
import scraper.types._
import scraper.utils._

sealed trait GeneratedNamedExpression extends NamedExpression {
  val purpose: Purpose

  override def name: String = purpose.name
}

object GeneratedNamedExpression {
  /**
   * Indicates the purpose of a [[GeneratedNamedExpression]].
   */
  sealed trait Purpose {
    val name: String
  }

  /**
   * Marks [[GeneratedNamedExpression]]s that are used to wrap/reference grouping expressions.
   */
  case object ForGrouping extends Purpose {
    override val name: String = "group"
  }

  /**
   * Marks [[GeneratedNamedExpression]]s that are used to wrap/reference aggregate functions.
   */
  case object ForAggregation extends Purpose {
    override val name: String = "agg"
  }
}

trait GeneratedAlias extends GeneratedNamedExpression with UnaryExpression {
  override def debugString: String = s"${child.debugString} AS g:${quote(name)}#${expressionID.id}"

  override def dataType: DataType = child.dataType

  override def isNullable: Boolean = child.isNullable

  override def isFoldable: Boolean = false

  def withID(id: ExpressionID): GeneratedAlias
}

trait GeneratedAttribute
  extends GeneratedNamedExpression
  with ResolvedAttribute
  with LeafExpression
  with UnevaluableExpression {

  override def debugString: String = "g:" + super.debugString
}

case class GroupingAlias(
  child: Expression,
  override val expressionID: ExpressionID = newExpressionID()
) extends GeneratedAlias {

  override val purpose: Purpose = ForGrouping

  override def toAttribute: GroupingAttribute =
    GroupingAttribute(child.dataType, child.isNullable, expressionID)

  override def withID(id: ExpressionID): GeneratedAlias = copy(expressionID = id)
}

case class GroupingAttribute(
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID
) extends GeneratedAttribute {

  override val purpose: Purpose = ForGrouping

  override def withID(id: ExpressionID): Attribute = copy(expressionID = id)
}

case class AggregationAlias(
  child: AggregateFunction,
  override val expressionID: ExpressionID = newExpressionID()
) extends GeneratedAlias {

  override val purpose: Purpose = ForAggregation

  override def toAttribute: AggregationAttribute =
    AggregationAttribute(child.dataType, child.isNullable, expressionID)

  override def withID(id: ExpressionID): GeneratedAlias = copy(expressionID = id)
}

case class AggregationAttribute(
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID
) extends GeneratedAttribute {

  override val purpose: Purpose = ForAggregation

  override def withID(id: ExpressionID): Attribute = copy(expressionID = id)
}
