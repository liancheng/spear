package scraper.expressions

import scraper.exceptions.ResolutionFailureException
import scraper.expressions.GeneratedNamedExpression.{ForAggregation, ForGrouping, Purpose}
import scraper.expressions.NamedExpression.newExpressionID
import scraper.plans.logical.LogicalPlan
import scraper.types._
import scraper.utils._

sealed trait GeneratedNamedExpression extends NamedExpression {
  def purpose: Purpose

  override def name: String = purpose.name
}

object GeneratedNamedExpression {
  /**
   * Indicates the purpose of a [[GeneratedNamedExpression]].
   */
  sealed trait Purpose {
    def name: String
  }

  /**
   * Marks [[GeneratedNamedExpression]]s that are used to wrap/reference grouping expressions.
   */
  case object ForGrouping extends Purpose {
    override def name: String = "group"
  }

  /**
   * Marks [[GeneratedNamedExpression]]s that are used to wrap/reference aggregate functions.
   */
  case object ForAggregation extends Purpose {
    override def name: String = "agg"
  }
}

trait GeneratedAlias extends GeneratedNamedExpression with UnaryExpression {
  override def debugString: String = s"${child.debugString} AS g:${quote(name)}#${expressionID.id}"

  override def dataType: DataType = child.dataType

  override def isNullable: Boolean = child.isNullable

  override def isFoldable: Boolean = false

  def withID(id: ExpressionID): GeneratedAlias
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

  /**
   * Returns the original expression to which this [[GeneratedAttribute]] refers in `plan`.
   */
  def origin(plan: LogicalPlan): Expression = {
    val candidates = plan.collectFromAllExpressions {
      case a: GeneratedAlias if a.expressionID == expressionID && a.purpose == purpose => a
    }

    candidates match {
      case Seq(a: GeneratedAlias) =>
        a.child

      case Nil =>
        throw new ResolutionFailureException(
          s"""Couldn't find the original expression referenced by attribute $this in plan
             |
             |${plan.prettyTree}
             |""".stripMargin
        )

      case _ =>
        throw new ResolutionFailureException({
          val candidateList = candidates mkString ("[", ", ", "]")
          s"""Attribute $this references multiple ambiguous expressions $candidateList in plan
             |
             |${plan.prettyTree}
             |""".stripMargin
        })
    }
  }
}

object GeneratedAttribute {
  def expand(expression: Expression, plan: LogicalPlan, purposes: Purpose*): Expression = {
    val attributes = expression.collect {
      case a: GeneratedAttribute if purposes contains a.purpose => a
    }

    val origins = attributes map (_ origin plan)
    val rewrite = (attributes zip origins).toMap

    expression transformDown {
      case a: GeneratedAttribute if purposes contains a.purpose => rewrite(a)
    }
  }

  implicit class ExpandGeneratedAttribute(expression: Expression) {
    def expand(plan: LogicalPlan, purposes: Purpose*): Expression = {
      GeneratedAttribute.expand(expression, plan, purposes: _*)
    }
  }
}

trait GroupingNamedExpression extends GeneratedNamedExpression {
  override def purpose: Purpose = ForGrouping
}

case class GroupingAlias(
  child: Expression,
  override val expressionID: ExpressionID = newExpressionID()
) extends GeneratedAlias with GroupingNamedExpression {
  override def toAttribute: GroupingAttribute = GroupingAttribute(this)

  override def withID(id: ExpressionID): GeneratedAlias = copy(expressionID = id)
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

  override def withID(id: ExpressionID): GeneratedAlias = copy(expressionID = id)
}

case class AggregationAttribute(alias: GeneratedAlias)
  extends GeneratedAttribute(alias) with AggregationNamedExpression
