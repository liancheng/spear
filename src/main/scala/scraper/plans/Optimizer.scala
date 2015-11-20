package scraper.plans

import scraper.Analyzer
import scraper.expressions.Literal.{False, True}
import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.logical.patterns.PhysicalOperation.{collectAliases, reduceAliases}
import scraper.plans.logical.{Filter, LogicalPlan, Project}
import scraper.trees.{Rule, RulesExecutor}

class Optimizer extends RulesExecutor[LogicalPlan] {
  override def batches: Seq[Batch] = Seq(
    Batch("Optimizations", FixedPoint.Unlimited, Seq(
      FoldConstants,
      FoldLogicalPredicates,
      ReduceNegations,
      NNFConversion,
      CNFConversion,
      SimplifyCasts,
      ReduceProjects,
      ReduceAliases
    ))
  )

  override def apply(tree: LogicalPlan): LogicalPlan = {
    assert(
      tree.resolved,
      s"""Logical query plan not resolved yet:
         |
         |${tree.prettyTree}
       """.stripMargin
    )

    logTrace(
      s"""Optimizing logical query plan:
         |
         |${tree.prettyTree}
       """.stripMargin
    )

    super.apply(tree)
  }

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
          case True Or _   => True
          case _ Or True   => True

          case False And _ => False
          case _ And False => False
        }
    }
  }

  /**
   * This rule eliminates unnecessary [[Not]] operators.
   */
  object ReduceNegations extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan =>
        plan transformExpressionsDown {
          case Not(Not(child))         => child
          case Not(lhs EqualTo rhs)    => lhs =/= rhs
          case Not(lhs NotEqualTo rhs) => lhs === rhs
        }
    }
  }

  /**
   * This rule eliminates unnecessary casts.  For example, implicit casts introduced by the
   * [[Analyzer]] may produce redundant casts.
   */
  object SimplifyCasts extends Rule[LogicalPlan] {
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
      case grandChild Project innerProjections Project outerProjections =>
        val collapsedProjections = {
          val childAliases = collectAliases(innerProjections)
          val aliasReducer = reduceAliases[NamedExpression](childAliases) _
          outerProjections map aliasReducer
        }

        Project(grandChild, collapsedProjections)
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
   * This rule converts a predicate to NNF (Negation Normal Form) using De Morgan's law.  NNF is
   * useful for converting the predicate one step further into CNF (Conjunctive Normal Form).
   *
   * Since we don't support existential/universal quantifiers or implications, this rule simply
   * pushes negations inwards by applying De Morgan's law.
   *
   * @see https://en.wikipedia.org/wiki/Negation_normal_form
   */
  object NNFConversion extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan: Filter =>
        plan transformExpressionsUp {
          case Not(lhs Or rhs)  => !lhs && !rhs
          case Not(lhs And rhs) => !lhs || !rhs
        }
    }
  }

  /**
   * This rule converts a predicate in NNF (Negation Normal Form) to CNF (Conjunctive Normal Form).
   *
   * Since we don't support existential/universal quantifiers or implications, this rule simply
   * distributes [[Or]]s inwards over [[And]]s.
   *
   * @see https://en.wikipedia.org/wiki/Conjunctive_normal_form
   */
  object CNFConversion extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan: Filter =>
        plan transformExpressionsUp {
          case (innerLhs And innerRhs) Or rhs => (innerLhs || rhs) && (innerRhs || rhs)
          case lhs Or (innerLhs And innerRhs) => (innerLhs || lhs) && (innerRhs || lhs)
        }
    }
  }
}
