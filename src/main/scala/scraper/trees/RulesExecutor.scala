package scraper.trees

import scala.annotation.tailrec

import scraper.trees.RulesExecutor.{EndCondition, FixedPoint, Once}
import scraper.utils.{Logging, sideBySide}

trait Rule[Base <: TreeNode[Base]] extends (Base => Base) {
  def apply(tree: Base): Base
}

trait RulesExecutor[Base <: TreeNode[Base]] extends Logging with (Base => Base) {
  case class RuleBatch(
    name: String,
    condition: EndCondition,
    rules: Seq[Rule[Base]]
  )

  def batches: Seq[RuleBatch]

  private def executeBatch(tree: Base, batch: RuleBatch): Base = {
    def executeRules(rules: Seq[Rule[Base]], tree: Base) = {
      rules.foldLeft(tree) {
        case (before, rule) =>
          val after = rule(before)
          val ruleName = rule.getClass.getSimpleName.stripSuffix("$")
          logTransformation(s"rule \'$ruleName\'", before, after)
          after
      }
    }

    @tailrec def untilFixedPoint(rules: Seq[Rule[Base]], tree: Base, maxIterations: Int): Base = {
      val transformed = executeRules(rules, tree)
      if (transformed.sameOrEqual(tree) || maxIterations == 1) {
        transformed
      } else {
        untilFixedPoint(rules, transformed, maxIterations - 1)
      }
    }

    val transformed = batch.condition match {
      case Once =>
        executeRules(batch.rules, tree)

      case FixedPoint(maxIterations) =>
        untilFixedPoint(batch.rules, tree, maxIterations)
    }

    logTransformation(s"rule batch \'${batch.name}\'", tree, transformed)
    transformed
  }

  def apply(tree: Base): Base = {
    batches.foldLeft(tree) { executeBatch }
  }

  private def logTransformation(transformation: String, before: Base, after: Base): Unit = {
    if (before sameOrEqual after) {
      logTrace(s"Applied $transformation, nothing changed")
    } else {
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
           |
           |$diff
           |""".stripMargin
      }
    }
  }
}

object RulesExecutor {
  sealed trait EndCondition {
    def maxIterations: Int
  }

  case object Once extends EndCondition {
    val maxIterations = 1
  }

  case class FixedPoint(maxIterations: Int) extends EndCondition {
    require(maxIterations != 0)
  }

  object FixedPoint {
    val Unlimited = FixedPoint(-1)
  }

}
