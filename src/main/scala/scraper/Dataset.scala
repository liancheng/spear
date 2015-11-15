package scraper

import java.io.PrintStream

import scraper.expressions.{ NamedExpression, Predicate }
import scraper.plans.logical.LogicalPlan
import scraper.plans.{ QueryExecution, logical }

class Dataset(val queryExecution: QueryExecution) {
  private def context: Context = queryExecution.context

  private def build(f: LogicalPlan => LogicalPlan): Dataset =
    new Dataset(context.execute(f(queryExecution.logicalPlan)))

  def select(expressions: NamedExpression*): Dataset = build(logical.Project(expressions, _))

  def filter(condition: Predicate): Dataset = build(logical.Filter(condition, _))

  def where(condition: Predicate): Dataset = this filter condition

  def iterator: Iterator[Row] = queryExecution.physicalPlan.iterator

  def toSeq: Seq[Row] = iterator.toSeq

  def explanation(extended: Boolean): String = if (extended) {
    s"""# Logical plan
       |${queryExecution.logicalPlan.prettyTree}
       |
       |# Analyzed plan
       |${queryExecution.analyzedPlan.prettyTree}
       |
       |# Optimized plan
       |${queryExecution.optimizedPlan.prettyTree}
       |
       |# Physical plan
       |${queryExecution.physicalPlan.prettyTree}
     """.stripMargin
  } else {
    s"""# Physical plan
       |${queryExecution.physicalPlan.prettyTree}
     """.stripMargin
  }
}
