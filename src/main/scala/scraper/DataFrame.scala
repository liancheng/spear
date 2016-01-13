package scraper

import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.expressions.{Ascending, Expression, SortOrder}
import scraper.plans.QueryExecution
import scraper.plans.logical._
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

  def select(first: Expression, rest: Expression*): DataFrame = this select (first +: rest)

  def select(expressions: Seq[Expression]): DataFrame = build(_ select expressions)

  def filter(condition: Expression): DataFrame = build(_ filter condition)

  def where(condition: Expression): DataFrame = this filter condition

  def limit(n: Expression): DataFrame = build(_ limit n)

  def limit(n: Int): DataFrame = this limit lit(n)

  def join(right: DataFrame): JoinedDataFrame = new JoinedDataFrame(this, right, Inner)

  def join(right: DataFrame, joinType: JoinType): DataFrame =
    new JoinedDataFrame(this, right, joinType)

  def leftJoin(right: DataFrame): DataFrame = new JoinedDataFrame(this, right, LeftOuter)

  def rightJoin(right: DataFrame): DataFrame = new JoinedDataFrame(this, right, RightOuter)

  def outerJoin(right: DataFrame): DataFrame = new JoinedDataFrame(this, right, FullOuter)

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

class JoinedDataFrame(left: DataFrame, right: DataFrame, joinType: JoinType) extends {
  private val join = {
    val leftPlan = left.queryExecution.logicalPlan
    val rightPlan = right.queryExecution.logicalPlan
    Join(leftPlan, rightPlan, joinType, None)
  }
} with DataFrame(join, left.context) {
  def on(condition: Expression): DataFrame =
    new DataFrame(join.copy(maybeCondition = Some(condition)), context)
}
