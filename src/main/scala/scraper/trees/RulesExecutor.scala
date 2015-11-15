package scraper.trees

import scala.annotation.tailrec

import scraper.utils.{ Logging, sideBySide }

trait Rule[Base <: TreeNode[Base]] {
  val name = getClass.getSimpleName.stripSuffix("$")

  def apply(tree: Base): Base
}

trait RulesExecutor[Base <: TreeNode[Base]] extends Logging {
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

  case class Batch(
    name: String,
    rules: Seq[Rule[Base]],
    condition: EndCondition
  )

  def batches: Seq[Batch]

  private def executeBatch(tree: Base, batch: Batch): Base = {
    def executeRules(rules: Seq[Rule[Base]], tree: Base) = {
      rules.foldLeft(tree) {
        case (before, rule) =>
          val after = rule(before)
          logTransformation(s"rule \'${rule.name}\'", before, after)
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

  def apply(tree: Base): Base = batches.foldLeft(tree) { executeBatch }

  private def logTransformation(transformation: String, before: Base, after: Base): Unit = {
    if (before sameOrEqual after) {
      logTrace(s"Applied $transformation, nothing changed")
    } else {
      logTrace {
        val diff = sideBySide(
          s"""Before $transformation
             |${before.prettyTree}
           """.stripMargin,

          s"""After $transformation
             |${after.prettyTree}
           """.stripMargin,

          withHeader = true
        )

        s"""Applied $transformation
           |$diff
         """.stripMargin.trim + "\n"
      }
    }
  }
}
