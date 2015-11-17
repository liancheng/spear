package scraper.plans

import scraper.expressions.Literal.{ False, True }
import scraper.expressions._
import scraper.plans.logical.LogicalPlan
import scraper.trees.{ Rule, RulesExecutor }
import scraper.types.BooleanType

class Optimizer extends RulesExecutor[LogicalPlan] {
  override def batches: Seq[Batch] = Seq(
    Batch(
      "Normalization",
      Seq(Normalization),
      Once
    ),

    Batch(
      "Optimizations",
      Seq(
        ConstantFolding,
        CastSimplification,
        BooleanSimplification
      ),
      FixedPoint.Unlimited
    )
  )

  override def apply(tree: LogicalPlan): LogicalPlan = {
    logTrace(
      s"""Optimizing logical query plan:
         |${tree.prettyTree}
       """.stripMargin
    )
    super.apply(tree)
  }

  object ConstantFolding extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.transformExpressionsUp {
      case e if e.foldable => Literal(e.evaluated)
    }
  }

  object CastSimplification extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.transformExpressionsUp {
      case Cast(from, to) if from.dataType == to => from
    }
  }

  object BooleanSimplification extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.transformExpressionsUp {
      case Not(True)                 => False
      case Not(False)                => True
      case Not(Not(child))           => child
      case Not(EqualTo(lhs, rhs))    => NotEqualTo(lhs, rhs)
      case Not(NotEqualTo(lhs, rhs)) => EqualTo(lhs, rhs)

      case And(e, False)             => False
      case Or(e, True)               => True
    }
  }

  object Normalization extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.transformExpressionsUp {
      case e @ And(_: Literal, _: Literal)        => e
      case e @ Or(_: Literal, _: Literal)         => e
      case e @ EqualTo(_: Literal, _: Literal)    => e
      case e @ NotEqualTo(_: Literal, _: Literal) => e

      case And(lhs: Literal, rhs)                 => And(rhs, lhs)
      case Or(lhs: Literal, rhs)                  => Or(rhs, lhs)
      case EqualTo(lhs: Literal, rhs)             => EqualTo(rhs, lhs)
      case NotEqualTo(lhs: Literal, rhs)          => NotEqualTo(rhs, lhs)
    }
  }
}
