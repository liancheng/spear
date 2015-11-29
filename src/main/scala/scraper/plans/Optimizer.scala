package scraper.plans

import scraper.{Analyzer, expressions}
import scraper.expressions.Literal.{False, True}
import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.Optimizer._
import scraper.plans.logical._
import scraper.plans.logical.patterns.PhysicalOperation.{collectAliases, reduceAliases}
import scraper.trees.{Rule, RulesExecutor}

class Optimizer extends RulesExecutor[LogicalPlan] {
  override def batches: Seq[RuleBatch] = Seq(
    RuleBatch("Optimizations", FixedPoint.Unlimited, Seq(
      FoldConstants,
      FoldLogicalPredicates,

      CNFConversion,

      ReduceAliases,
      ReduceCasts,
      ReduceFilters,
      ReduceLimits,
      ReduceNegations,
      ReduceProjects,

      PushFiltersThroughProjects,
      PushProjectsThroughLimits
    ))
  )

  override def apply(tree: LogicalPlan): LogicalPlan = {
    assert(
      tree.resolved,
      s"""Logical query plan not resolved yet:
         |
         |${tree.prettyTree}
         |""".stripMargin
    )

    logTrace(
      s"""Optimizing logical query plan:
         |
         |${tree.prettyTree}
         |""".stripMargin
    )

    super.apply(tree)
  }
}

object Optimizer {
  /**
   * This rule finds all foldable expressions and evaluate them to literals.
   */
  object FoldConstants extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan =>
        plan transformExpressionsUp {
          case e if e.foldable => lit(e.evaluated)
        }
    }
  }

  /**
   * This rule simplifies logical predicates containing `TRUE` and/or `FALSE`.
   */
  object FoldLogicalPredicates extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan =>
        plan transformExpressionsDown {
          case True Or _                => True
          case _ Or True                => True

          case False And _              => False
          case _ And False              => False

          case If(True, trueValue, _)   => trueValue
          case If(False, _, falseValue) => falseValue
        }
    }
  }

  /**
   * This rule eliminates unnecessary [[expressions.Not]] operators.
   */
  object ReduceNegations extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan =>
        plan transformExpressionsDown {
          case Not(Not(child))    => child
          case Not(lhs Eq rhs)    => lhs =/= rhs
          case Not(lhs NotEq rhs) => lhs =:= rhs

          case Not(lhs Gt rhs)    => lhs <= rhs
          case Not(lhs GtEq rhs)  => lhs < rhs
          case Not(lhs Lt rhs)    => lhs >= rhs
          case Not(lhs LtEq rhs)  => lhs > rhs

          case If(Not(c), t, f)   => If(c, f, t)
        }
    }
  }

  /**
   * This rule eliminates unnecessary casts.  For example, implicit casts introduced by the
   * [[Analyzer]] may produce redundant casts.
   */
  object ReduceCasts extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan =>
        plan transformExpressionsDown {
          case e Cast t if e.dataType == t => e
          case e Cast _ Cast t             => e cast t
        }
    }
  }

  /**
   * This rule reduces adjacent projects.  Aliases are also inlined/substituted when possible.
   */
  object ReduceProjects extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case plan Project projections if projections == plan.output =>
        plan

      case plan Project innerProjections Project outerProjections =>
        val aliases = collectAliases(innerProjections)
        plan select (outerProjections map (reduceAliases(aliases, _)))
    }
  }

  /**
   * This rule reduces adjacent aliases.
   */
  object ReduceAliases extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case plan =>
        plan transformExpressionsDown {
          case outer @ Alias(_, inner: Alias, _) => outer.copy(child = inner.child)
        }
    }
  }

  /**
   * This rule converts a predicate to CNF (Conjunctive Normal Form).
   *
   * Since we don't support existential/universal quantifiers or implications, this rule simply
   * pushes negations inwards by applying De Morgan's law and distributes [[expressions.Or]]s
   * inwards over [[expressions.And]]s.
   *
   * @see https://en.wikipedia.org/wiki/Conjunctive_normal_form
   */
  object CNFConversion extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan: Filter =>
        plan transformExpressionsUp {
          case Not(lhs Or rhs)                => !lhs && !rhs
          case Not(lhs And rhs)               => !lhs || !rhs
          case (innerLhs And innerRhs) Or rhs => (innerLhs || rhs) && (innerRhs || rhs)
          case lhs Or (innerLhs And innerRhs) => (innerLhs || lhs) && (innerRhs || lhs)
        }
    }
  }

  /**
   * This rule combines adjacent [[logical.Filter]]s into a single [[logical.Filter]].
   */
  object ReduceFilters extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Filter True               => plan
      case plan Filter False              => EmptyRelation
      case plan Filter inner Filter outer => plan filter inner && outer
    }
  }

  /**
   * This rule pushes [[logical.Filter]] predicates beneath [[logical.Project]]s.
   */
  object PushFiltersThroughProjects extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Project projections Filter condition =>
        plan filter reduceAliases(collectAliases(projections), condition) select projections
    }
  }

  object PushProjectsThroughLimits extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Limit n Project projections =>
        plan select projections limit n
    }
  }

  object ReduceLimits extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Limit n Limit m => Limit(plan, If(n < m, n, m))
    }
  }
}
