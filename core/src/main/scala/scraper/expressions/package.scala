package scraper

import scraper.expressions.GeneratedNamedExpression.{ForAggregation, ForGrouping}
import scraper.expressions.NamedExpression.newExpressionID

package object expressions {
  type GroupingAlias = GeneratedAlias[ForGrouping.type]

  object GroupingAlias {
    def apply(child: Expression): GroupingAlias =
      GeneratedAlias(ForGrouping, child, newExpressionID())

    def unapply(e: GeneratedAlias[_]): Option[GroupingAlias] =
      if (e.purpose == ForGrouping) Some(e.asInstanceOf[GroupingAlias]) else None
  }

  type GroupingAttribute = GeneratedAttribute[ForGrouping.type]

  object GroupingAttribute {
    def unapply(e: GeneratedAttribute[_]): Option[GroupingAttribute] =
      if (e.purpose == ForGrouping) Some(e.asInstanceOf[GroupingAttribute]) else None
  }

  type AggregateAlias = GeneratedAlias[ForAggregation.type]

  object AggregateAlias {
    def apply(child: AggregateFunction): AggregateAlias =
      GeneratedAlias(ForAggregation, child, newExpressionID())

    def unapply(e: GeneratedAlias[_]): Option[AggregateAlias] =
      if (e.purpose == ForAggregation) Some(e.asInstanceOf[AggregateAlias]) else None
  }

  type AggregateAttribute = GeneratedAttribute[ForAggregation.type]

  object AggregateAttribute {
    def unapply(e: GeneratedAttribute[_]): Option[AggregateAttribute] =
      if (e.purpose == ForAggregation) Some(e.asInstanceOf[AggregateAttribute]) else None
  }
}
