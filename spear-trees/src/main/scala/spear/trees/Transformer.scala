package spear.trees

import scala.annotation.tailrec

import spear.utils.{sideBySide, Logging}

trait RuleLike[Base <: TreeNode[Base]] extends (Base => Base) with Logging {
  protected def logTransformation(action: String, before: Base)(tree: => Base): Base = {
    val after = tree

    if (!before.same(after)) {
      logTrace {
        val diff = sideBySide(
          s"""Before $action
             |${before.prettyTree}
             |""".stripMargin,

          s"""After $action
             |${after.prettyTree}
             |""".stripMargin,

          withHeader = true
        )

        s"""Applied $action
           |$diff
           |""".stripMargin
      }
    }

    after
  }
}

trait Rule[Base <: TreeNode[Base]] extends RuleLike[Base] {
  private val name = getClass.getSimpleName stripSuffix "$"

  override final def apply(tree: Base): Base = logTransformation(s"rule '$name'", tree) {
    transform(tree)
  }

  def transform(tree: Base): Base
}

case class RuleGroup[Base <: TreeNode[Base]](
  convergenceTest: ConvergenceTest,
  rules: Seq[Base => Base]
) extends RuleLike[Base] {

  require(rules.nonEmpty)

  override def apply(tree: Base): Base = converge(tree)

  @tailrec private def converge(before: Base): Base = {
    val after = composedRules(before)
    if (convergenceTest.test(before, after)) after else converge(after)
  }

  private val composedRules = rules reduce { _ andThen _ }
}

class Transformer[Base <: TreeNode[Base]](rules: Seq[Base => Base]) extends RuleLike[Base] {
  require(rules.nonEmpty)

  def this(first: RuleGroup[Base], rest: RuleGroup[Base]*) = this(first +: rest)

  def apply(tree: Base): Base = composedRules(tree)

  private val composedRules = rules reduce { _ andThen _ }
}

sealed trait ConvergenceTest {
  def test[Base <: TreeNode[Base]](before: Base, after: Base): Boolean
}

case object Once extends ConvergenceTest {
  def test[Base <: TreeNode[Base]](before: Base, after: Base): Boolean = true
}

case object FixedPoint extends ConvergenceTest {
  def test[Base <: TreeNode[Base]](before: Base, after: Base): Boolean = before same after
}
