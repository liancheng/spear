package scraper.expressions

import scraper.Name
import scraper.expressions.InternalNamedExpression.{ForAggregation, ForGrouping, ForWindow, Purpose}
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

trait GroupingNamedExpression extends InternalNamedExpression {
  override def purpose: Purpose = ForGrouping
}

case class GroupingAlias(
  child: Expression,
  override val expressionID: ExpressionID = newExpressionID()
) extends GroupingNamedExpression with InternalAlias {
  override def attr: GroupingAttribute = GroupingAttribute(
    purpose, dataType, isNullable, expressionID
  )

  override def withID(id: ExpressionID): GroupingAlias = copy(expressionID = id)
}

case class GroupingAttribute(
  override val purpose: Purpose,
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID
) extends GroupingNamedExpression with InternalAttribute {
  override def withID(id: ExpressionID): Attribute = copy(expressionID = id)
}

trait AggregationNamedExpression extends InternalNamedExpression {
  override def purpose: Purpose = ForAggregation
}

case class AggregationAlias(
  child: AggregateFunction,
  override val expressionID: ExpressionID = newExpressionID()
) extends AggregationNamedExpression with InternalAlias {
  override def attr: AggregationAttribute =
    AggregationAttribute(purpose, dataType, isNullable, expressionID)

  override def withID(id: ExpressionID): AggregationAlias = copy(expressionID = id)
}

case class AggregationAttribute(
  override val purpose: Purpose,
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID
) extends AggregationNamedExpression with InternalAttribute {
  override def withID(id: ExpressionID): Attribute = copy(expressionID = id)
}

trait WindowNamedExpression extends InternalNamedExpression {
  override def purpose: Purpose = ForWindow
}

case class WindowAlias(
  child: WindowFunction,
  override val expressionID: ExpressionID = newExpressionID()
) extends WindowNamedExpression with InternalAlias {
  override def attr: InternalAttribute =
    WindowAttribute(purpose, dataType, isNullable, expressionID)

  override def withID(id: ExpressionID): WindowAlias = copy(expressionID = id)
}

case class WindowAttribute(
  override val purpose: Purpose,
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID
) extends WindowNamedExpression with InternalAttribute {
  override def withID(id: ExpressionID): Attribute = copy(expressionID = id)
}
