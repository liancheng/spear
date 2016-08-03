package scraper

import scala.util.control.NonFatal

import org.scalatest.FunSuite

import scraper.expressions._
import scraper.plans.QueryPlan
import scraper.plans.logical.LogicalPlan
import scraper.trees.TreeNode
import scraper.types.DataType
import scraper.utils._

trait TestUtils { this: FunSuite =>
  private[scraper] def assertSideBySide(actual: String, expected: String): Unit = {
    if (expected != actual) {
      fail(sideBySide(
        s"""Actual
           |$expected
           |""".stripMargin,

        s"""Expected
           |$actual
           |""".stripMargin,

        withHeader = true
      ))
    }
  }

  private[scraper] def assertSideBySide[T <: TreeNode[T]](
    actual: TreeNode[T],
    expected: TreeNode[T]
  ): Unit = {
    if (expected != actual) {
      fail(sideBySide(
        s"""Actual
           |${expected.prettyTree}
           |""".stripMargin,

        s"""Expected
           |${actual.prettyTree}
           |""".stripMargin,

        withHeader = true
      ))
    }
  }

  private[scraper] def checkTree[T <: TreeNode[T]](
    actual: TreeNode[T],
    expected: TreeNode[T]
  ): Unit = {
    assertSideBySide(actual, expected)
  }

  private def normalizeExpressionId[Plan <: QueryPlan[Plan]](plan: Plan): Plan = {
    val allIdExpressions = plan.collectFromAllExpressions {
      case e @ (_: Alias | _: AttributeRef | _: GeneratedNamedExpression) => e
    }.distinct

    val rewrite = allIdExpressions.zipWithIndex.toMap

    plan.transformAllExpressions {
      case e: Alias              => e withID ExpressionID(rewrite(e))
      case e: AttributeRef       => e withID ExpressionID(rewrite(e))
      case e: GeneratedAlias     => e withID ExpressionID(rewrite(e))
      case e: GeneratedAttribute => e withID ExpressionID(rewrite(e))
    }
  }

  def checkPlan[Plan <: QueryPlan[Plan]](actual: Plan, expected: Plan): Unit =
    checkTree(normalizeExpressionId(expected), normalizeExpressionId(actual))

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
        s"""Actual answer:
           |${actual mkString "\n"}
           |""".stripMargin,

        s"""Expected answer:
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

  def checkWellTyped(plan: LogicalPlan): Unit = {
    if (!plan.isWellTyped) {
      fail(
        s"""Logical plan not well-typed:
           |${plan.prettyTree}
           |""".stripMargin
      )
    }
  }

  def checkStrictlyTyped(plan: LogicalPlan): Unit = {
    if (!plan.isWellTyped) {
      fail(
        s"""Logical plan not well-typed:
           |${plan.prettyTree}
           |""".stripMargin
      )
    }

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
    context.catalog.removeRelation(name)
  }

  def withTable(name: Name)(f: => Unit)(implicit context: Context): Unit =
    withTable(context, name)(f)
}
