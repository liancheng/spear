package scraper

import scraper.expressions.GeneratedNamedExpression.{ForAggregation, ForGrouping}

package object expressions {
  type GroupingAlias = GeneratedAlias[ForGrouping.type]

  type GroupingAttribute = GeneratedAttribute[ForGrouping.type]

  type AggregateAlias = GeneratedAlias[ForAggregation.type]

  type AggregateAttribute = GeneratedAttribute[ForAggregation.type]
}
