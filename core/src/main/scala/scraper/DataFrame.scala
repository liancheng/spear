package scraper

import java.io.PrintStream

import scraper.config.Keys.NullsLarger
import scraper.exceptions.ResolutionFailureException
import scraper.expressions.AutoAlias.named
import scraper.expressions._
import scraper.expressions.dsl._
import scraper.expressions.functions._
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

  def apply(column: String): Attribute =
    queryExecution.analyzedPlan.output find (_.name == column) getOrElse {
      throw new ResolutionFailureException(s"Failed to resolve column name $column")
    }

  def apply(column: Symbol): Attribute = this(column.name)

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

  def having(condition: Expression): DataFrame = this filter condition

  def limit(n: Expression): DataFrame = withPlan(_ limit n)

  def limit(n: Int): DataFrame = this limit lit(n)

  def distinct: DataFrame = withPlan(Distinct)

  def join(right: DataFrame): JoinedDataFrame = new JoinedDataFrame(this, right, Inner)

  def join(right: DataFrame, joinType: JoinType): JoinedDataFrame =
    new JoinedDataFrame(this, right, joinType)

  def leftJoin(right: DataFrame): JoinedDataFrame = new JoinedDataFrame(this, right, LeftOuter)

  def rightJoin(right: DataFrame): JoinedDataFrame = new JoinedDataFrame(this, right, RightOuter)

  def outerJoin(right: DataFrame): JoinedDataFrame = new JoinedDataFrame(this, right, FullOuter)

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

  def groupBy(keys: Seq[Expression]): GroupedData = new GroupedData(this, keys)

  def groupBy(first: Expression, rest: Expression*): GroupedData = groupBy(first +: rest)

  def agg(projectList: Seq[Expression]): DataFrame = this groupBy Nil agg projectList

  def agg(first: Expression, rest: Expression*): DataFrame = agg(first +: rest)

  def iterator: Iterator[Row] = queryExecution.physicalPlan.iterator

  def asTable(tableName: String): Unit =
    context.catalog.registerRelation(tableName, queryExecution.analyzedPlan)

  def asTable(tableName: Symbol): Unit = asTable(tableName.name)

  def toSeq: Seq[Row] = iterator.toSeq

  def toArray: Array[Row] = iterator.toArray

  def foreach(f: Row => Unit): Unit = iterator foreach f

  def showSchema(out: PrintStream = System.out): Unit = out.println(schema.prettyTree)

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

  def explain(extended: Boolean = true, out: PrintStream = System.out): Unit =
    out.println(explanation(extended))

  def show(rowCount: Int = 20, truncate: Boolean = true, out: PrintStream = System.out): Unit =
    out.println(tabulate(rowCount, truncate))

  private def tabulate(rowCount: Int = 20, truncate: Boolean = true): String = {
    val truncated = limit(rowCount + 1).toArray
    val hasMoreData = truncated.length > rowCount
    val data = truncated take rowCount

    val rows = schema.fields.map(_.name) +: data.map { row =>
      row.map { cell =>
        val content = cell match {
          case null => "NULL"
          case _    => cell.toString
        }

        if (truncate && content.length > 20) (content take 17) + "..." else content
      }
    }.toSeq

    tabulate(rows, rowCount, truncate, hasMoreData)
  }

  private def tabulate(
    rows: Seq[Seq[String]], rowCount: Int, truncate: Boolean, hasMoreData: Boolean
  ): String = {
    val builder = StringBuilder.newBuilder

    // TODO This is slow for large datasets
    val columnWidths = rows.transpose map (_.map(_.length).max)

    val bar = "\u2500"
    val thickBar = "\u2550"
    val pipe = "\u2502"
    val cross = "\u253c"

    val upperLeft = "\u2552"
    val upperRight = "\u2555"
    val lowerLeft = "\u2558"
    val lowerRight = "\u255b"

    val leftTee = "\u251c"
    val rightTee = "\u2524"
    val upperTee = "\u2564"
    val lowerTee = "\u2567"

    val upper = columnWidths map (thickBar * _) mkString (upperLeft, upperTee, upperRight + "\n")
    val middle = columnWidths map (bar * _) mkString (leftTee, cross, rightTee + "\n")
    val lower = columnWidths map (thickBar * _) mkString (lowerLeft, lowerTee, lowerRight + "\n")

    def displayRow(row: Seq[String]): String = row zip columnWidths map {
      case (name, width) if truncate => " " * (width - name.length) + name
      case (name, width)             => name padTo (width, ' ')
    } mkString (pipe, pipe, pipe + "\n")

    val body = rows map displayRow

    builder ++= upper
    builder ++= body.head
    builder ++= middle
    body.tail foreach builder.append
    builder ++= lower

    if (hasMoreData) {
      builder ++= s"Only showing top $rowCount row(s)"
    }

    builder.toString()
  }
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

class GroupedData(df: DataFrame, keys: Seq[Expression]) {
  def agg(projectList: Seq[Expression]): DataFrame =
    df.withPlan(UnresolvedAggregate(_, keys, projectList map named))

  def agg(first: Expression, rest: Expression*): DataFrame = agg(first +: rest)
}
