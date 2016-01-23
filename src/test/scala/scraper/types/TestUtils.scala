package scraper.types

import org.scalatest.FunSuite

import scraper.expressions.{Alias, AttributeRef, ExpressionId}
import scraper.plans.QueryPlan
import scraper.trees.TreeNode
import scraper.utils._
import scraper.{DataFrame, Row}

trait TestUtils { this: FunSuite =>
  private[scraper] def assertSideBySide(expected: String, actual: String): Unit = {
    if (expected != actual) {
      fail(sideBySide(
        s"""Expected
           |$expected
           |""".stripMargin,

        s"""Actual
           |$actual
           |""".stripMargin,

        withHeader = true
      ))
    }
  }

  private[scraper] def checkTree[T <: TreeNode[T]](
    expected: TreeNode[T],
    actual: TreeNode[T]
  ): Unit = {
    assertSideBySide(expected.prettyTree, actual.prettyTree)
  }

  private def normalizeExpressionId[Plan <: QueryPlan[Plan]](plan: Plan): Plan = {
    var normalizedId = -1L

    plan.transformExpressionsUp {
      case e: AttributeRef =>
        normalizedId += 1
        e.copy(expressionId = ExpressionId(normalizedId))

      case e: Alias =>
        normalizedId += 1
        e.copy(expressionId = ExpressionId(normalizedId))
    }
  }

  private[scraper] def checkPlan[Plan <: QueryPlan[Plan]](actual: Plan, expected: Plan): Unit = {
    checkTree(normalizeExpressionId(expected), normalizeExpressionId(actual))
  }

  private[scraper] def checkDataFrame(actual: DataFrame, expected: DataFrame): Unit =
    checkDataFrame(actual, expected.toSeq)

  private[scraper] def checkDataFrame(ds: DataFrame, expected: Row): Unit =
    checkDataFrame(ds, expected :: Nil)

  private[scraper] def checkDataFrame(ds: DataFrame, expected: => Seq[Row]): Unit = {
    val actual = ds.queryExecution.physicalPlan.iterator.toSeq
    if (actual != expected) {
      val explanation = ds.explanation(extended = true)

      val answerDiff = sideBySide(
        s"""Expected answer:
           |${expected mkString "\n"}
           |""".stripMargin,

        s"""Actual answer:
           |${actual mkString "\n"}
           |""".stripMargin,

        withHeader = true
      )

      fail(
        s"""Unexpected row(s) detected:
           |
           |$answerDiff
           |
           |Query plan details:
           |
           |$explanation
           |""".stripMargin
      )
    }
  }
}
