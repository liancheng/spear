package scraper

import scraper.config.Keys.NullsLarger
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.expressions._
import scraper.plans.QueryExecution
import scraper.plans.logical._
import scraper.plans.logical.dsl._
import scraper.types.StructType

class DataFrame(val queryExecution: QueryExecution) {
  def this(logicalPlan: LogicalPlan, context: Context) = this(context execute logicalPlan)

  def context: Context = queryExecution.context

  private[scraper] def withPlan(f: LogicalPlan => LogicalPlan): DataFrame =
    new DataFrame(f(queryExecution.logicalPlan), context)

  lazy val schema: StructType = StructType fromAttributes queryExecution.analyzedPlan.output

  def rename(newNames: String*): DataFrame = {
    assert(newNames.length == schema.fields.length)
    val oldNames = schema.fields map (_.name)
    val aliases = (oldNames, newNames).zipped map { Symbol(_) as _ }
    this select aliases
  }

  def select(first: Expression, rest: Expression*): DataFrame = this select (first +: rest)

  def select(expressions: Seq[Expression]): DataFrame = withPlan(_ select expressions)

  def filter(condition: Expression): DataFrame = withPlan(_ filter condition)

  def where(condition: Expression): DataFrame = this filter condition

  def limit(n: Expression): DataFrame = withPlan(_ limit n)

  def limit(n: Int): DataFrame = this limit lit(n)

  def join(right: DataFrame): JoinedDataFrame = new JoinedDataFrame(this, right, Inner)

  def join(right: DataFrame, joinType: JoinType): DataFrame =
    new JoinedDataFrame(this, right, joinType)

  def leftJoin(right: DataFrame): DataFrame = new JoinedDataFrame(this, right, LeftOuter)

  def rightJoin(right: DataFrame): DataFrame = new JoinedDataFrame(this, right, RightOuter)

  def outerJoin(right: DataFrame): DataFrame = new JoinedDataFrame(this, right, FullOuter)

  def orderBy(first: Expression, rest: Expression*): DataFrame = {
    val sortOrders = first +: rest map (SortOrder(_, Ascending, context.settings(NullsLarger)))
    withPlan(Sort(_, sortOrders))
  }

  def orderBy(first: SortOrder, rest: SortOrder*): DataFrame = withPlan(Sort(_, first +: rest))

  def subquery(name: String): DataFrame = withPlan(_ subquery name)

  def subquery(name: Symbol): DataFrame = subquery(name.name)

  def as(name: String): DataFrame = subquery(name)

  def as(name: Symbol): DataFrame = subquery(name.name)

  def union(that: DataFrame): DataFrame = withPlan(_ union that.queryExecution.logicalPlan)

  def intersect(that: DataFrame): DataFrame = withPlan(_ intersect that.queryExecution.logicalPlan)

  def except(that: DataFrame): DataFrame = withPlan(_ except that.queryExecution.logicalPlan)

  def groupBy(groupingList: Seq[Expression]): GroupedData =
    new GroupedData(this, groupingList map (GroupingAlias(_)))

  def groupBy(first: Expression, rest: Expression*): GroupedData = groupBy(first +: rest)

  def iterator: Iterator[Row] = queryExecution.physicalPlan.iterator

  def registerAsTable(tableName: String): Unit =
    context.catalog.registerRelation(tableName, queryExecution.analyzedPlan)

  def toSeq: Seq[Row] = iterator.toSeq

  def toArray: Array[Row] = iterator.toArray

  def foreach(f: Row => Unit): Unit = iterator foreach f

  def explanation(extended: Boolean = true): String = if (extended) {
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

  def explain(extended: Boolean = true): Unit = println(explanation(extended))
}

class JoinedDataFrame(left: DataFrame, right: DataFrame, joinType: JoinType) extends {
  private val join = {
    val leftPlan = left.queryExecution.logicalPlan
    val rightPlan = right.queryExecution.logicalPlan
    Join(leftPlan, rightPlan, joinType, None)
  }
} with DataFrame(join, left.context) {
  def on(condition: Expression): DataFrame =
    new DataFrame(join.copy(condition = Some(condition)), context)
}

class GroupedData(df: DataFrame, groupingList: Seq[GroupingAlias]) {
  def agg(aggregateList: Seq[NamedExpression]): DataFrame =
    df.withPlan(Aggregate(_, groupingList, aggregateList))

  def agg(first: NamedExpression, rest: NamedExpression*): DataFrame = agg(first +: rest)
}
