package scraper.expressions

import scraper.Name
import scraper.expressions.GeneratedNamedExpression.{ForAggregation, ForGrouping, Purpose}
import scraper.expressions.NamedExpression.newExpressionID
import scraper.types._
import scraper.utils._

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
    s"(${child.debugString} AS g:${quote(name.toString)}#${expressionID.id})"

  override def dataType: DataType = child.dataType

  override def isNullable: Boolean = child.isNullable

  override lazy val isFoldable: Boolean = false

  override def toAttribute: GeneratedAttribute

  def withID(id: ExpressionID): GeneratedAlias
}

object GeneratedAlias {
  def collectAliases(
    expressions: Seq[NamedExpression],
    purposes: Purpose*
  ): Map[GeneratedAttribute, Expression] =
    expressions
      .collect { case a: GeneratedAlias if purposes contains a.purpose => a }
      .map(a => a.toAttribute -> a.child)
      .toMap

  def betaReduction(
    expression: Expression,
    aliases: Map[GeneratedAttribute, Expression]
  ): Expression = expression.transformUp {
    case a: GeneratedAttribute => aliases.getOrElse(a, a)
  }

  def betaReduction(
    expression: Expression,
    targets: Seq[NamedExpression],
    purposes: Purpose*
  ): Expression = betaReduction(expression, collectAliases(targets, purposes: _*))
}

abstract class GeneratedAttribute(alias: GeneratedAlias)
  extends GeneratedNamedExpression
  with ResolvedAttribute
  with LeafExpression
  with UnevaluableExpression {

  require(alias.purpose == this.purpose)

  override def dataType: DataType = alias.dataType

  override def isNullable: Boolean = alias.isNullable

  override def expressionID: ExpressionID = alias.expressionID

  override def withID(id: ExpressionID): Attribute = (alias withID id).toAttribute

  override def debugString: String = "g:" + super.debugString
}

trait GroupingNamedExpression extends GeneratedNamedExpression {
  override def purpose: Purpose = ForGrouping
}

case class GroupingAlias(
  child: Expression,
  override val expressionID: ExpressionID = newExpressionID()
) extends GeneratedAlias with GroupingNamedExpression {
  override def toAttribute: GroupingAttribute = GroupingAttribute(this)

  override def withID(id: ExpressionID): GroupingAlias = copy(expressionID = id)
}

case class GroupingAttribute(alias: GeneratedAlias)
  extends GeneratedAttribute(alias) with GroupingNamedExpression

trait AggregationNamedExpression extends GeneratedNamedExpression {
  override def purpose: Purpose = ForAggregation
}

case class AggregationAlias(
  child: AggregateFunction,
  override val expressionID: ExpressionID = newExpressionID()
) extends GeneratedAlias with AggregationNamedExpression {
  override def toAttribute: AggregationAttribute = AggregationAttribute(this)

  override def withID(id: ExpressionID): AggregationAlias = copy(expressionID = id)
}

case class AggregationAttribute(alias: GeneratedAlias)
  extends GeneratedAttribute(alias) with AggregationNamedExpression
