package scraper

import scraper.expressions._
import scraper.plans.logical.Aggregate

class GroupedData(df: DataFrame, groupingExprs: Seq[Expression]) {

  private[this] def alias(expr: Expression): NamedExpression = expr match {
    case expr: NamedExpression => expr
    case expr: Expression      => Alias(expr.getClass.getSimpleName, expr)
  }

  private[this] def toDF(aggExprs: Seq[Expression]): DataFrame = {
    val aliased = aggExprs.map(alias)
    new DataFrame(Aggregate(groupingExprs, aliased, df.queryExecution.logicalPlan), df.context)
  }

  def agg(expr: Expression, exprs: Expression*): DataFrame = toDF(expr +: exprs)
}

object GroupedData {
  def count() = Count(Literal(1))
  def sum(expr: Expression) = Sum(expr)
  def max(expr: Expression) = Max(expr)
  def min(expr: Expression) = Min(expr)
}