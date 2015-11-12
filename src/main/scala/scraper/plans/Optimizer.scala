package scraper.plans

import scraper.expressions._
import scraper.plans.logical.LogicalPlan
import scraper.trees.{ Rule, RulesExecutor }
import scraper.types.BooleanType

class Optimizer extends RulesExecutor[LogicalPlan] {
  override def batches: Seq[Batch] = Seq(
    Batch(
      "normalization",
      Seq(Normalization),
      Once
    ),

    Batch(
      "optimizations",
      Seq(
        ConstantFolding,
        CastSimplification,
        BooleanSimplification
      ),
      FixedPoint.Unlimited
    )
  )

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
      case Not(Literal(true, BooleanType))     => Literal(false)
      case Not(Literal(false, BooleanType))    => Literal(true)
      case Not(Not(child))                     => child
      case Not(EqualTo(lhs, rhs))              => NotEqualTo(lhs, rhs)
      case Not(NotEqualTo(lhs, rhs))           => EqualTo(lhs, rhs)

      case And(e, Literal(false, BooleanType)) => Literal(false)
      case Or(e, Literal(true, BooleanType))   => Literal(true)
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
