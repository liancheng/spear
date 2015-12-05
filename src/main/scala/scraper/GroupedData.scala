package scraper

import scraper.expressions._
import scraper.plans.logical.Aggregate

class GroupedData(df: DataFrame, groupingExprs: Seq[Expression]) {
  private[this] def alias(expr: Expression): NamedExpression = expr match {
    case expr: NamedExpression => expr
    case expr: Expression      => Alias(expr.getClass.getSimpleName, expr)
  }

  private[this] def toDF(aggs: Seq[Expression]): DataFrame = {
    val aliased = aggs.map(alias)
    new DataFrame(Aggregate(df.queryExecution.logicalPlan, groupingExprs, aliased), df.context)
  }

  def agg(expr: Expression, exprs: Expression*): DataFrame = toDF(expr +: exprs)
}
