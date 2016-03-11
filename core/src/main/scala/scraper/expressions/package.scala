package scraper

import scraper.expressions.GeneratedNamedExpression.Grouping

package object expressions {
  type GroupingAlias = GeneratedAlias[Grouping.type]

  type GroupingAttribute = GeneratedAttribute[Grouping.type]
}
