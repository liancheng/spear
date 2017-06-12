package spear

import scala.util.control.NonFatal

import org.scalatest.FunSuite
import spear.expressions._
import spear.plans.QueryPlan
import spear.plans.logical.LogicalPlan
import spear.trees.{TreeNode, TreeTest}
import spear.trees.utils._
import spear.types.DataType

trait TestUtils extends TreeTest { this: FunSuite =>
  def checkPlan[Plan <: QueryPlan[Plan]](actual: Plan, expected: Plan): Unit =
    checkTree(QueryPlan.normalizeExpressionIDs(actual), QueryPlan.normalizeExpressionIDs(expected))

  def checkDataFrame(actual: DataFrame, expected: DataFrame): Unit =
    checkDataFrame(actual, expected.toSeq)

  def checkDataFrame(df: DataFrame, first: Row, rest: Row*): Unit =
    checkDataFrame(df, first +: rest)

  def checkDataFrame(df: DataFrame, expected: Seq[Row]): Unit = {
    val actual = try {
      df.queryExecution.physicalPlan.iterator.toSeq
    } catch {
      case NonFatal(cause) =>
        fail(
          s"""Query execution failed:
             |
             |${df.explanation(extended = true)}
             |""".stripMargin,
          cause
        )
    }

    if (actual != expected) {
      val answerDiff = sideBySide(
        s"""Actual
           |${actual mkString "\n"}
           |""".stripMargin,

        s"""Expected
           |${expected mkString "\n"}
           |""".stripMargin,

        withHeader = true
      )

      fail(
        s"""Unexpected row(s) detected:
           |$answerDiff
           |
           |Query plan details:
           |
           |${df explanation (extended = true)}
           |""".stripMargin
      )
    }
  }

  def checkWellTyped(e: Expression, dataType: DataType): Unit = {
    if (!e.isWellTyped) {
      fail(
        s"""Expression ${e.debugString} is not well-typed:
           |${e.prettyTree}
           |""".stripMargin
      )
    }

    val actualType = e.strictlyTyped.get.dataType
    if (actualType != dataType) {
      fail(
        s"""Strictly typed form of ${e.debugString} has wrong data type $actualType:
           |${e.strictlyTyped.get.prettyTree}
           |""".stripMargin
      )
    }
  }

  def checkStrictlyTyped(e: Expression, dataType: DataType): Unit = {
    if (!e.isStrictlyTyped) {
      fail(
        s"""Expression ${e.debugString} is not strictly-typed:
           |${e.prettyTree}
           |""".stripMargin
      )
    }

    val actualType = e.strictlyTyped.get.dataType
    if (actualType != dataType) {
      fail(
        s"""Strictly typed form of ${e.debugString} has wrong data type $actualType:
           |${e.prettyTree}
           |""".stripMargin
      )
    }
  }

  def checkWellTyped(plan: LogicalPlan): Unit =
    if (!plan.isWellTyped) {
      fail(
        s"""Logical plan not well-typed:
           |${plan.prettyTree}
           |""".stripMargin
      )
    }

  def checkStrictlyTyped(plan: LogicalPlan): Unit = {
    checkWellTyped(plan)

    if (!plan.isStrictlyTyped) {
      fail(
        s"""Logical plan is well typed but not strictly-typed:
           |
           |# Original logical plan:
           |${plan.prettyTree}
           |
           |# Well-typed logical plan:
           |${plan.strictlyTyped.get.prettyTree}
           |""".stripMargin
      )
    }
  }

  def withTable(context: Context, name: Name)(f: => Unit): Unit = try f finally {
    context.queryExecutor.catalog.removeRelation(name)
  }

  def withTable(name: Name)(f: => Unit)(implicit context: Context): Unit =
    withTable(context, name)(f)
}
