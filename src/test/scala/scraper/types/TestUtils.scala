package scraper.types

import org.scalatest.FunSuite
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

  private[scraper] def assertSideBySide[T <: TreeNode[T]](
    expected: TreeNode[T],
    actual: TreeNode[T]
  ): Unit = {
    assertSideBySide(expected.prettyTree, actual.prettyTree)
  }
}
