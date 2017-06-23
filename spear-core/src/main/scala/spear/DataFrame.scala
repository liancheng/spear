package spear

import java.io.PrintStream

import spear.expressions._
import spear.expressions.functions._
import spear.plans.QueryExecution
import spear.plans.logical._
import spear.types.StructType

class DataFrame(val queryExecution: QueryExecution) {
  // Analyzes the query plan eagerly to provide early error detection.
  queryExecution.analyzedPlan

  def this(logicalPlan: LogicalPlan, context: Context) =
    this(context.queryExecutor.execute(context, logicalPlan))

  def context: Context = queryExecution.context

  lazy val schema: StructType = StructType fromAttributes queryExecution.analyzedPlan.output

  def rename(newNames: Name*): DataFrame = {
    assert(newNames.length == schema.fields.length)
    val oldNames = schema.fields map { _.name }
    val aliases = (oldNames, newNames).zipped map { _ as _ }
    this select aliases
  }

  def select(first: Expression, rest: Expression*): DataFrame = this select (first +: rest)

  def select(expressions: Seq[Expression]): DataFrame = withPlan { _ select expressions }

  def filter(condition: Expression): DataFrame = withPlan { _ filter condition }

  def limit(n: Expression): DataFrame = withPlan { _ limit n }

  def limit(n: Int): DataFrame = this limit lit(n)

  def distinct: DataFrame = withPlan { Distinct(_)() }

  def crossJoin(right: DataFrame): DataFrame = withPlan {
    _ join (right.queryExecution.logicalPlan, Inner)
  }

  def join(right: DataFrame): JoinedData = new JoinedData(this, right, Inner)

  def leftJoin(right: DataFrame): JoinedData = new JoinedData(this, right, LeftOuter)

  def rightJoin(right: DataFrame): JoinedData = new JoinedData(this, right, RightOuter)

  def outerJoin(right: DataFrame): JoinedData = new JoinedData(this, right, FullOuter)

  def orderBy(order: Seq[Expression]): DataFrame = withPlan {
    _ sort (order map SortOrder.apply)
  }

  def orderBy(first: Expression, rest: Expression*): DataFrame = orderBy(first +: rest)

  def subquery(name: Name): DataFrame = withPlan {
    _ subquery name
  }

  def union(that: DataFrame): DataFrame = withPlan {
    _ union that.queryExecution.logicalPlan
  }

  def intersect(that: DataFrame): DataFrame = withPlan {
    _ intersect that.queryExecution.logicalPlan
  }

  def except(that: DataFrame): DataFrame = withPlan {
    _ except that.queryExecution.logicalPlan
  }

  def groupBy(keys: Seq[Expression]): GroupedData = GroupedData(this, keys)

  def groupBy(first: Expression, rest: Expression*): GroupedData = groupBy(first +: rest)

  def agg(projectList: Seq[Expression]): DataFrame = this groupBy Nil agg projectList

  def agg(first: Expression, rest: Expression*): DataFrame = agg(first +: rest)

  def iterator: Iterator[Row] = queryExecution.physicalPlan.iterator

  def asTable(tableName: Name): Unit =
    context.queryExecutor.catalog.registerRelation(tableName, queryExecution.analyzedPlan)

  def toSeq: Seq[Row] = if (queryExecution.physicalPlan.requireMaterialization) {
    iterator.map { _.copy() }.toSeq
  } else {
    iterator.toSeq
  }

  def showSchema(out: PrintStream = System.out): Unit = out.println(schema.prettyTree)

  def explanation(extended: Boolean = true): String = if (extended) {
    s"""## Parsed logical plan ##
       |${queryExecution.logicalPlan.prettyTree}
       |
       |## Analyzed logical plan ##
       |${queryExecution.analyzedPlan.prettyTree}
       |
       |## Optimized logical plan ##
       |${queryExecution.optimizedPlan.prettyTree}
       |
       |## Physical plan ##
       |${queryExecution.physicalPlan.prettyTree}
       |""".stripMargin
  } else {
    s"""## Physical plan ##
       |${queryExecution.physicalPlan.prettyTree}
       |""".stripMargin
  }

  def explain(extended: Boolean = false, out: PrintStream = System.out): Unit =
    out.println(explanation(extended))

  def explainExtended(out: PrintStream = System.out): Unit = explain(extended = true, out)

  def show(rowCount: Int = 20, truncate: Boolean = true, out: PrintStream = System.out): Unit =
    show(Some(rowCount), truncate, out)

  private[spear] def withPlan(f: LogicalPlan => LogicalPlan): DataFrame =
    new DataFrame(f(queryExecution.analyzedPlan), context)

  def show(rowCount: Option[Int], truncate: Boolean, out: PrintStream): Unit =
    out.println(tabulate(rowCount, truncate))

  private def tabulate(rowCount: Option[Int] = Some(20), truncate: Boolean = true): String = {
    val (data, hasMoreData) = rowCount map { n =>
      val truncated = limit(n + 1).toSeq
      val hasMoreData = truncated.length > n
      val data = truncated take n
      (data, hasMoreData)
    } getOrElse {
      (toSeq, false)
    }

    val rows = schema.fields.map { _.name.casePreserving } +: data.map {
      _ map { cell =>
        val content = cell match {
          case null => "NULL"
          case _    => cell.toString
        }

        if (truncate && content.length > 20) (content take 17) + "..." else content
      }
    }

    tabulate(rows, data.length, truncate, hasMoreData)
  }

  private def tabulate(
    rows: Seq[Seq[String]], rowCount: Int, truncate: Boolean, hasMoreData: Boolean
  ): String = {
    val builder = StringBuilder.newBuilder

    // TODO This is slow for large datasets
    val columnWidths = rows.transpose map { _.map { _.length }.max + 2 }

    // format: OFF
    val topBorder    = columnWidths.map { "═" * _ } mkString ("╒", "╤", "╕\n")
    val separator    = columnWidths.map { "─" * _ } mkString ("├", "┼", "┤\n")
    val bottomBorder = columnWidths.map { "═" * _ } mkString ("╘", "╧", "╛\n")
    // format: ON

    def displayRow(row: Seq[String]): String = row zip columnWidths map {
      case (content, width) if truncate => " " * (width - content.length - 1) + content + " "
      case (content, width)             => " " + content.padTo(width - 2, ' ') + " "
    } mkString ("│", "│", "│" + "\n")

    val body = rows map displayRow

    builder ++= topBorder
    builder ++= body.head
    builder ++= separator
    body.tail foreach builder.append
    builder ++= bottomBorder

    if (hasMoreData) {
      builder ++= s"Only showing top $rowCount row(s)"
    }

    builder.toString()
  }
}

class JoinedData(left: DataFrame, right: DataFrame, joinType: JoinType) {
  def on(condition: Expression): DataFrame = {
    val leftPlan = left.queryExecution.logicalPlan
    val rightPlan = right.queryExecution.logicalPlan
    val join = leftPlan join (rightPlan, joinType) on condition
    new DataFrame(join, left.context)
  }
}

case class GroupedData(child: DataFrame, keys: Seq[Expression]) {
  def agg(projectList: Seq[Expression]): DataFrame = child.withPlan {
    _ groupBy keys agg projectList
  }

  def agg(first: Expression, rest: Expression*): DataFrame = agg(first +: rest)
}
