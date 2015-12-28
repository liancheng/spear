package scraper

import scraper.exceptions.ContractBrokenException
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.expressions.{Ascending, Expression, SortOrder}
import scraper.plans.QueryExecution
import scraper.plans.logical.{Inner, Join, LogicalPlan, Sort}
import scraper.types.StructType

class DataFrame(val queryExecution: QueryExecution) {
  def this(logicalPlan: LogicalPlan, context: Context) = this(context execute logicalPlan)

  def context: Context = queryExecution.context

  private def build(f: LogicalPlan => LogicalPlan): DataFrame =
    new DataFrame(f(queryExecution.logicalPlan), context)

  lazy val schema: StructType = StructType fromAttributes queryExecution.analyzedPlan.output

  def rename(newNames: String*): DataFrame = {
    assert(newNames.length == schema.fields.length)
    val oldNames = schema.fields map (_.name)
    val aliases = (oldNames, newNames).zipped map { Symbol(_) as _ }
    this select aliases
  }

  def sql: Option[String] = queryExecution.analyzedPlan.sql(context)

  def select(first: Expression, rest: Expression*): DataFrame = this select (first +: rest)

  def select(expressions: Seq[Expression]): DataFrame = build(_ select expressions)

  def filter(condition: Expression): DataFrame = build(_ filter condition)

  def where(condition: Expression): DataFrame = this filter condition

  def limit(n: Expression): DataFrame = build(_ limit n)

  def limit(n: Int): DataFrame = this limit lit(n)

  def join(right: DataFrame): DataFrame = build {
    Join(_, right.queryExecution.logicalPlan, Inner, None)
  }

  def join(right: DataFrame, condition: Expression): DataFrame = build {
    Join(_, right.queryExecution.logicalPlan, Inner, Some(condition))
  }

  def on(condition: Expression): DataFrame = build {
    case join: Join => join on condition
    case _ => throw new ContractBrokenException(
      s"${getClass.getSimpleName}.on() can only be applied over join operators."
    )
  }

  def groupBy(expr: Expression*): GroupedData = new GroupedData(this, expr)

  def agg(expr: Expression, exprs: Expression*): DataFrame = this groupBy () agg (expr, exprs: _*)

  def orderBy(expr: Expression, exprs: Expression*): DataFrame = build { plan =>
    Sort(plan, (expr +: exprs).map(SortOrder(_, Ascending)))
  }

  def iterator: Iterator[Row] = queryExecution.physicalPlan.iterator

  def registerAsTable(tableName: String): Unit =
    context.catalog.registerRelation(tableName, queryExecution.analyzedPlan)

  def toSeq: Seq[Row] = iterator.toSeq

  def toArray: Array[Row] = iterator.toArray

  def foreach(f: Row => Unit): Unit = iterator foreach f

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
       |""".stripMargin
  } else {
    s"""# Physical plan
       |${queryExecution.physicalPlan.prettyTree}
       |""".stripMargin
  }

  def explain(extended: Boolean): Unit = println(explanation(extended))

  def explain(): Unit = println(explanation(extended = true))
}
