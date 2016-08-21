package scraper.expressions

import scraper.Name
import scraper.expressions.GeneratedNamedExpression.{ForAggregation, ForGrouping, Purpose}
import scraper.expressions.NamedExpression.newExpressionID
import scraper.expressions.aggregates.AggregateFunction
import scraper.types._

sealed trait GeneratedNamedExpression extends NamedExpression {
  def purpose: Purpose

  override def name: Name = purpose.name
}

object GeneratedNamedExpression {
  /**
   * Indicates the purpose of a [[GeneratedNamedExpression]].
   */
  sealed trait Purpose {
    def name: Name
  }

  /**
   * Marks [[GeneratedNamedExpression]]s that are used to wrap/reference grouping expressions.
   */
  case object ForGrouping extends Purpose {
    override def name: Name = 'group
  }

  /**
   * Marks [[GeneratedNamedExpression]]s that are used to wrap/reference aggregate functions.
   */
  case object ForAggregation extends Purpose {
    override def name: Name = 'agg
  }
}

trait GeneratedAlias extends GeneratedNamedExpression with UnaryExpression {
  override def debugString: String =
    s"(${child.debugString} AS g:${name.toString}#${expressionID.id})"

  override def dataType: DataType = child.dataType

  override def isNullable: Boolean = child.isNullable

  override lazy val isFoldable: Boolean = false

  override def toAttribute: GeneratedAttribute

  def withID(id: ExpressionID): GeneratedAlias
}

object GeneratedAlias {
  /**
   * Collects all [[GeneratedAlias]]es with the given `purposes` from `expressions`.
   */
  private def collectAliases(
    expressions: Seq[NamedExpression],
    purposes: Purpose*
  ): Map[GeneratedAttribute, Expression] = expressions.collect {
    case a: GeneratedAlias if purposes contains a.purpose => a
  }.map(a => a.toAttribute -> a.child).toMap

  def inlineAliases(
    expression: Expression,
    aliases: Map[GeneratedAttribute, Expression]
  ): Expression = expression.transformUp {
    case a: GeneratedAttribute => aliases.getOrElse(a, a)
  }

  def inlineAliases(
    expression: Expression,
    targets: Seq[NamedExpression],
    purposes: Purpose*
  ): Expression = inlineAliases(expression, collectAliases(targets, purposes: _*))
}

abstract class GeneratedAttribute extends GeneratedNamedExpression
  with ResolvedAttribute
  with LeafExpression
  with UnevaluableExpression {

  override def debugString: String = "g:" + super.debugString
}

trait GroupingNamedExpression extends GeneratedNamedExpression {
  override def purpose: Purpose = ForGrouping
}

case class GroupingAlias(
  child: Expression,
  override val expressionID: ExpressionID = newExpressionID()
) extends GeneratedAlias with GroupingNamedExpression {
  override def toAttribute: GroupingAttribute = GroupingAttribute(
    purpose, dataType, isNullable, expressionID
  )

  override def withID(id: ExpressionID): GroupingAlias = copy(expressionID = id)
}

case class GroupingAttribute(
  override val purpose: Purpose,
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID
) extends GeneratedAttribute with GroupingNamedExpression {
  override def withID(id: ExpressionID): Attribute = copy(expressionID = id)
}

trait AggregationNamedExpression extends GeneratedNamedExpression {
  override def purpose: Purpose = ForAggregation
}

case class AggregationAlias(
  child: AggregateFunction,
  override val expressionID: ExpressionID = newExpressionID()
) extends GeneratedAlias with AggregationNamedExpression {
  override def toAttribute: AggregationAttribute =
    AggregationAttribute(purpose, dataType, isNullable, expressionID)

  override def withID(id: ExpressionID): AggregationAlias = copy(expressionID = id)
}

case class AggregationAttribute(
  override val purpose: Purpose,
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID
) extends GeneratedAttribute with AggregationNamedExpression {
  override def withID(id: ExpressionID): Attribute = copy(expressionID = id)
}
