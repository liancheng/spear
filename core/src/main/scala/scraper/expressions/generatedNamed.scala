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

  def newInstance(id: ExpressionID = newExpressionID()): GeneratedAlias
}

trait GeneratedAttribute
  extends GeneratedNamedExpression
  with ResolvedAttribute
  with LeafExpression
  with UnevaluableExpression

case class GroupingAlias private (
  child: Expression,
  override val expressionID: ExpressionID
) extends GeneratedAlias {

  override val purpose: Purpose = ForGrouping

  override def toAttribute: GroupingAttribute =
    GroupingAttribute(child.dataType, child.isNullable, expressionID)

  override def newInstance(id: ExpressionID): GeneratedAlias =
    copy(expressionID = newExpressionID())
}

object GroupingAlias {
  def apply(child: Expression): GroupingAlias = child match {
    case e: GroupingAlias => e
    case e                => GroupingAlias(e, newExpressionID())
  }
}

case class GroupingAttribute(
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID
) extends GeneratedAttribute {

  override val purpose: Purpose = ForGrouping

  override def newInstance(id: ExpressionID): Attribute = copy(expressionID = newExpressionID())

  override def withNullability(nullable: Boolean): GroupingAttribute =
    copy(isNullable = nullable)
}

case class AggregationAlias private (
  child: AggregateFunction,
  override val expressionID: ExpressionID
) extends GeneratedAlias {

  override val purpose: Purpose = ForAggregation

  override def toAttribute: AggregationAttribute =
    AggregationAttribute(child.dataType, child.isNullable, expressionID)

  override def newInstance(id: ExpressionID): GeneratedAlias =
    copy(expressionID = newExpressionID())
}

object AggregationAlias {
  def apply(child: AggregateFunction): AggregationAlias = AggregationAlias(child, newExpressionID())
}

case class AggregationAttribute(
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID
) extends GeneratedAttribute {

  override val purpose: Purpose = ForAggregation

  override def newInstance(id: ExpressionID): Attribute = copy(expressionID = newExpressionID())

  override def withNullability(nullable: Boolean): AggregationAttribute =
    copy(isNullable = nullable)
}
