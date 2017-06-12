package spear.trees

import org.scalatest.FunSuite

import spear.trees.utils.sideBySide

trait TreeTest { this: FunSuite =>
  def assertSideBySide(actual: String, expected: String): Unit = {
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

  def assertSideBySide[T <: TreeNode[T]](actual: TreeNode[T], expected: TreeNode[T]): Unit =
    if (actual != expected) {
      fail(sideBySide(
        s"""Actual
           |${actual.prettyTree}
           |""".stripMargin,

        s"""Expected
           |${expected.prettyTree}
           |""".stripMargin,

        withHeader = true
      ))
    }

  def checkTree[T <: TreeNode[T]](actual: TreeNode[T], expected: TreeNode[T]): Unit =
    assertSideBySide(actual, expected)

}
