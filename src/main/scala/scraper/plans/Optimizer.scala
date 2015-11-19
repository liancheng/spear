package scraper.plans

import scraper.expressions.Literal.{ False, True }
import scraper.expressions._
import scraper.plans.logical.LogicalPlan
import scraper.trees.{ Rule, RulesExecutor }

class Optimizer extends RulesExecutor[LogicalPlan] {
  override def batches: Seq[Batch] = Seq(
    Batch("Normalization", Once, Seq(Normalization)),

    Batch("Optimizations", FixedPoint.Unlimited, Seq(
      ConstantFolding,
      CastSimplification,
      BooleanSimplification
    ))
  )

  override def apply(tree: LogicalPlan): LogicalPlan = {
    logTrace(
      s"""Optimizing logical query plan:
         |
         |${tree.prettyTree}
       """.stripMargin
    )
    super.apply(tree)
  }

  object ConstantFolding extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree.transformUp {
      case plan =>
        plan.transformExpressionsUp {
          case e if e.foldable => Literal(e.evaluated)
        }
    }
  }

  object CastSimplification extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree.transformUp {
      case plan =>
        plan.transformExpressionsUp {
          case Cast(from, to) if from.dataType == to => from
        }
    }
  }

  object BooleanSimplification extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree.transformUp {
      case plan =>
        plan.transformExpressionsUp {
          case Not(True)          => False
          case Not(False)         => True
          case Not(e: Not)        => e.child
          case Not(e: EqualTo)    => NotEqualTo(e.left, e.right)
          case Not(e: NotEqualTo) => EqualTo(e.left, e.right)
          case And(_, False)      => False
          case Or(_, True)        => True
        }
    }
  }

  object Normalization extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree.transformUp {
      case plan =>
        plan.transformExpressionsUp {
          case e @ And(_: Literal, _: Literal)        => e
          case And(lhs: Literal, rhs)                 => And(rhs, lhs)

          case e @ Or(_: Literal, _: Literal)         => e
          case Or(lhs: Literal, rhs)                  => Or(rhs, lhs)

          case e @ EqualTo(_: Literal, _: Literal)    => e
          case EqualTo(lhs: Literal, rhs)             => EqualTo(rhs, lhs)

          case e @ NotEqualTo(_: Literal, _: Literal) => e
          case NotEqualTo(lhs: Literal, rhs)          => NotEqualTo(rhs, lhs)
        }
    }
  }
}
