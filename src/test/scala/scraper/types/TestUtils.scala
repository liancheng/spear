package scraper.types

import org.scalatest.FunSuite
import scraper.expressions.{ Alias, AttributeRef, ExpressionId }
import scraper.plans.QueryPlan
import scraper.trees.TreeNode
import scraper.utils._

class TestUtils extends FunSuite {
  private[scraper] def assertSideBySide(expected: String, actual: String): Unit = {
    if (expected != actual) {
      fail(sideBySide(
        s"""Expected
           |$expected
         """.stripMargin,

        s"""Actual
           |$actual
         """.stripMargin,

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

    plan.transformExpressionsDown {
      case e: AttributeRef =>
        normalizedId += 1
        e.copy(expressionId = ExpressionId(normalizedId))

      case e: Alias =>
        normalizedId += 1
        e.copy(expressionId = ExpressionId(normalizedId))
    }
  }

  private[scraper] def checkPlan[Plan <: QueryPlan[Plan]](expected: Plan, actual: Plan): Unit = {
    checkTree(normalizeExpressionId(expected), normalizeExpressionId(actual))
  }
}
