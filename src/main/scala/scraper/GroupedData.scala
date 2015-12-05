package scraper

import scraper.expressions._
import scraper.plans.logical.Aggregate

class GroupedData(df: DataFrame, groupingExprs: Seq[Expression]) {
  private[this] def toDF(aggs: Seq[Expression]): DataFrame = {
    val aliased = aggs.map(_ match {
      case e: NamedExpression => e
      case e                  => Alias(e.getClass.getSimpleName, e)
    })
    new DataFrame(Aggregate(df.queryExecution.logicalPlan, groupingExprs, aliased), df.context)
  }

  def agg(expr: Expression, exprs: Expression*): DataFrame = toDF(expr +: exprs)
}
