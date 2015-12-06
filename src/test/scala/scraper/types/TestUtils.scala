package scraper.types

import org.scalatest.FunSuite

import scraper.expressions.{Alias, AttributeRef, ExpressionId}
import scraper.plans.QueryPlan
import scraper.trees.TreeNode
import scraper.utils._
import scraper.{DataFrame, Row}

trait TestUtils { this: FunSuite =>
  private[scraper] def assertSideBySide(expected: String, actual: String): Boolean = {
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

    true
  }

  private[scraper] def checkTree[T <: TreeNode[T]](
    expected: TreeNode[T],
    actual: TreeNode[T]
  ): Boolean = {
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

  private[scraper] def checkPlan[Plan <: QueryPlan[Plan]](expected: Plan, actual: Plan): Boolean = {
    checkTree(normalizeExpressionId(expected), normalizeExpressionId(actual))
  }

  private[scraper] def checkDataFrame(ds: DataFrame, expected: Row): Boolean =
    checkDataFrame(ds, expected :: Nil)

  private[scraper] def checkDataFrame(ds: DataFrame, expected: => Seq[Row]): Boolean = {
    val actual = ds.queryExecution.physicalPlan.iterator.toSeq
    if (actual != expected) {
      val explanation = ds.explain(extended = true)

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
           |Query plan details:
           |
           |$explanation
           |""".stripMargin
      )
    }

    true
  }
}
