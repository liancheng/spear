package spear.trees

import scala.annotation.tailrec

import spear.utils.{sideBySide, Logging}

trait TreeTransformation[Base <: TreeNode[Base]] extends (Base => Base) with Logging {
  protected def logTransformation(transformation: String, before: Base)(after: => Base): Base = {
    if (!before.same(after)) {
      logTrace {
        val diff = sideBySide(
          s"""Before $transformation
             |${before.prettyTree}
             |""".stripMargin,

          s"""After $transformation
             |${after.prettyTree}
             |""".stripMargin,

          withHeader = true
        )

        s"""Applied $transformation
           |$diff
           |""".stripMargin
      }
    }

    after
  }
}

trait Rule[Base <: TreeNode[Base]] extends TreeTransformation[Base] {
  private val name = getClass.getSimpleName stripSuffix "$"

  override final def apply(tree: Base): Base = logTransformation(s"rule '$name'", tree) {
    transform(tree)
  }

  def transform(tree: Base): Base
}

case class Phase[Base <: TreeNode[Base]](
  name: String,
  convergenceTest: ConvergenceTest,
  rules: Seq[Rule[Base]]
) extends TreeTransformation[Base] {

  override def apply(tree: Base): Base = logTransformation(s"phase '$name'", tree) {
    applyUntilConvergent(tree)
  }

  @tailrec private def applyUntilConvergent(before: Base): Base = {
    val after = rules.foldLeft(before) { (tree, rule) => rule(tree) }
    if (convergenceTest.test(before, after)) after else applyUntilConvergent(after)
  }
}

class Transformer[Base <: TreeNode[Base]](phases: Seq[Phase[Base]])
  extends TreeTransformation[Base] {

  def this(first: Phase[Base], rest: Phase[Base]*) = this(first +: rest)

  def apply(before: Base): Base = phases.foldLeft(before) { (tree, phase) => phase(tree) }
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
