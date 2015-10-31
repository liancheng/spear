package scraper.trees

import scala.annotation.tailrec

trait Rule[Base <: TreeNode[Base]] {
  def apply(tree: Base): Base
}

trait RulesExecutor[Base <: TreeNode[Base]] {
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
      rules.foldLeft(tree) { case (toBeTransformed, rule) => rule(toBeTransformed) }
    }

    @tailrec def untilFixedPoint(rules: Seq[Rule[Base]], tree: Base, maxIterations: Int): Base = {
      val transformed = executeRules(rules, tree)
      if (transformed.sameOrEqual(tree) || maxIterations == 1) transformed
      else untilFixedPoint(rules, transformed, maxIterations - 1)
    }

    batch.condition match {
      case Once =>
        executeRules(batch.rules, tree)

      case FixedPoint(maxIterations) =>
        untilFixedPoint(batch.rules, tree, maxIterations)
    }
  }

  def apply(tree: Base): Base = batches.foldLeft(tree) { executeBatch }
}
