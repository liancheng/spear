package scraper.expressions

import scraper.Name
import scraper.expressions.InternalNamedExpression._
import scraper.expressions.NamedExpression.newExpressionID
import scraper.expressions.aggregates.AggregateFunction
import scraper.expressions.windows.WindowFunction
import scraper.types._

sealed trait InternalNamedExpression extends NamedExpression with NonSQLExpression {
  def purpose: Purpose

  override def name: Name = purpose.name
}

object InternalNamedExpression {
  /**
   * Indicates the purpose of a [[InternalNamedExpression]].
   */
  sealed trait Purpose {
    def name: Name
  }

  /**
   * Marks [[InternalNamedExpression]]s that are used to wrap/reference grouping expressions.
   */
  case object ForGrouping extends Purpose {
    override def name: Name = 'G
  }

  /**
   * Marks [[InternalNamedExpression]]s that are used to wrap/reference aggregate functions.
   */
  case object ForAggregation extends Purpose {
    override def name: Name = 'A
  }

  case object ForWindow extends Purpose {
    override def name: Name = 'W
  }

  case object ForAggState extends Purpose {
    override def name: Name = 'S
  }
}

trait InternalAlias extends UnaryExpression with InternalNamedExpression {
  override def debugString: String =
    s"(${child.debugString} AS @${name.toString}#${expressionID.id})"

  override def dataType: DataType = child.dataType

  override def isNullable: Boolean = child.isNullable

  override lazy val isFoldable: Boolean = false

  override def attr: InternalAttribute
}

object InternalAlias {
  def unaliasUsing[E <: Expression](
    targets: Seq[NamedExpression], purposes: Purpose*
  )(expression: E): E = {
    val aliases = targets collect {
      case a: InternalAlias if purposes contains a.purpose =>
        (a.attr: Expression) -> a.child
    }

    expression.transformUp(aliases.toMap).asInstanceOf[E]
  }

  def buildRewriter(aliases: Seq[InternalAlias]): Map[Expression, Expression] =
    aliases.map { a => a.child -> (a.attr: Expression) }.toMap

  def buildRestorer(aliases: Seq[InternalAlias]): Map[Expression, Expression] =
    aliases.map { a => (a.attr: Expression) -> a.child }.toMap
}

trait InternalAttribute extends LeafExpression
  with ResolvedAttribute
  with UnevaluableExpression
  with InternalNamedExpression {

  override def debugString: String = "@" + super.debugString
}

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

case class AggStateAttribute private (
  override val name: Name,
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID
) extends InternalAttribute {
  override val purpose: Purpose = ForAggState

  override def withID(id: ExpressionID): AggStateAttribute = copy(expressionID = id)
}

case object AggStateAttribute {
  def apply(ref: AttributeRef): AggStateAttribute = AggStateAttribute(
    ForAggState.name append "[" append ref.name.casePreserving append "]",
    ref.dataType,
    ref.isNullable,
    ref.expressionID
  )
}
