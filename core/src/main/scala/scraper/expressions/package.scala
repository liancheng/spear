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

  type AggregationAlias = GeneratedAlias[ForAggregation.type]

  object AggregationAlias {
    def apply(child: AggregateFunction): AggregationAlias =
      GeneratedAlias(ForAggregation, child, newExpressionID())

    def unapply(e: GeneratedAlias[_]): Option[AggregationAlias] =
      if (e.purpose == ForAggregation) Some(e.asInstanceOf[AggregationAlias]) else None
  }

  type AggregationAttribute = GeneratedAttribute[ForAggregation.type]

  object AggregationAttribute {
    def unapply(e: GeneratedAttribute[_]): Option[AggregationAttribute] =
      if (e.purpose == ForAggregation) Some(e.asInstanceOf[AggregationAttribute]) else None
  }
}
