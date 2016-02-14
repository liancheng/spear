package scraper.plans.logical

import scraper.expressions
import scraper.expressions.Literal.{False, True}
import scraper.expressions.Predicate.{splitConjunction, toCNF}
import scraper.expressions._
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.plans.logical
import scraper.plans.logical.Optimizer._
import scraper.plans.logical.patterns.PhysicalOperation.{collectAliases, reduceAliases}
import scraper.trees.RulesExecutor.FixedPoint
import scraper.trees.{Rule, RulesExecutor}

class Optimizer extends RulesExecutor[LogicalPlan] {
  override def batches: Seq[RuleBatch] = Seq(
    RuleBatch("Optimizations", FixedPoint.Unlimited, Seq(
      FoldConstants,
      FoldLogicalPredicates,

      CNFConversion,
      EliminateCommonPredicates,

      ReduceAliases,
      ReduceCasts,
      ReduceFilters,
      ReduceLimits,
      ReduceNegations,
      ReduceProjects,

      PushFiltersThroughProjects,
      PushFiltersThroughJoins,
      PushProjectsThroughLimits
    ))
  )

  override def apply(tree: LogicalPlan): LogicalPlan = {
    assert(
      tree.resolved,
      s"""Logical query plan not resolved yet:
         |${tree.prettyTree}
         |""".stripMargin
    )

    logTrace(
      s"""Optimizing logical query plan:
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
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressions {
      case e if e.foldable => lit(e.evaluated)
    }
  }

  /**
   * This rule simplifies logical predicates containing `TRUE` and/or `FALSE`.
   */
  object FoldLogicalPredicates extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressions {
      case True || _                 => True
      case _ || True                 => True

      case False && _                => False
      case _ && False                => False

      case a && b if a sameOrEqual b => a
      case a || b if a sameOrEqual b => a

      case If(True, yes, _)          => yes
      case If(False, _, no)          => no
    }
  }

  /**
   * This rule eliminates unnecessary [[expressions.Not Not]] operators.
   */
  object ReduceNegations extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressions {
      case !(!(child))                  => child
      case !(lhs =:= rhs)               => lhs =/= rhs
      case !(lhs =/= rhs)               => lhs =:= rhs

      case !(lhs > rhs)                 => lhs <= rhs
      case !(lhs >= rhs)                => lhs < rhs
      case !(lhs < rhs)                 => lhs >= rhs
      case !(lhs <= rhs)                => lhs > rhs

      case If(!(c), t, f)               => If(c, f, t)

      case a && !(b) if a sameOrEqual b => False
      case a || !(b) if a sameOrEqual b => True

      case !(IsNull(child))             => IsNotNull(child)
      case !(IsNotNull(child))          => IsNull(child)
    }
  }

  /**
   * This rule eliminates unnecessary casts.  For example, implicit casts introduced by the
   * [[Analyzer]] may produce redundant casts.
   */
  object ReduceCasts extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressions {
      case e Cast t if e.dataType == t => e
      case e Cast _ Cast t             => e cast t
    }
  }

  /**
   * This rule reduces adjacent projects.  Aliases are also inlined/substituted when possible.
   */
  object ReduceProjects extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Project projectList if projectList == plan.output =>
        plan

      case plan Project inner Project outer =>
        val aliases = collectAliases(inner)
        plan select (outer map (reduceAliases(aliases, _)))
    }
  }

  /**
   * This rule reduces adjacent aliases.
   */
  object ReduceAliases extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressions {
      case outer @ Alias(Alias(child, _, _), _, _) => outer.copy(child = child)
    }
  }

  /**
   * This rule converts a predicate to CNF (Conjunctive Normal Form).
   *
   * Since we don't support existential/universal quantifiers or implications, this rule simply
   * pushes negations inwards by applying De Morgan's law and distributes [[expressions.Or Or]]s
   * inwards over [[expressions.And And]]s.
   *
   * @see https://en.wikipedia.org/wiki/Conjunctive_normal_form
   */
  object CNFConversion extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Filter condition => plan filter toCNF(condition)
    }
  }

  object EliminateCommonPredicates extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressions {
      case lhs && rhs if lhs == rhs            => lhs
      case lhs || rhs if lhs == rhs            => lhs
      case If(condition, yes, no) if yes == no => Coalesce(condition, yes)
    }
  }

  /**
   * This rule combines adjacent [[logical.Filter Filter]]s into a single [[logical.Filter Filter]].
   */
  object ReduceFilters extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Filter inner Filter outer => plan filter (inner && outer)
    }
  }

  object FoldConstantFilters extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Filter True  => plan
      case plan Filter False => LocalRelation(Nil, plan.output)
    }
  }

  /**
   * This rule pushes [[logical.Filter Filter]]s beneath [[logical.Project Project]]s.
   */
  object PushFiltersThroughProjects extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Project projectList Filter condition =>
        val substitutedCondition = reduceAliases(collectAliases(projectList), condition)
        plan filter substitutedCondition select projectList
    }
  }

  /**
   * This rule pushes [[logical.Filter Filter]]s beneath [[logical.Join Join]]s whenever possible.
   */
  object PushFiltersThroughJoins extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case (join @ Join(left, right, Inner, maybeCondition)) Filter condition =>
        val (leftPredicates, rest) = splitConjunction(toCNF(condition)).partition {
          _.references subsetOf left.references
        }

        val (rightPredicates, commonPredicates) = rest.partition {
          _.references subsetOf right.references
        }

        def applyPredicates(predicates: Seq[Expression], plan: LogicalPlan) =
          predicates reduceOption (_ && _) map plan.filter getOrElse plan

        join.copy(
          left = applyPredicates(leftPredicates, left),
          right = applyPredicates(rightPredicates, right),
          maybeCondition = (maybeCondition ++ commonPredicates) reduceOption (_ && _)
        )
    }
  }

  object PushProjectsThroughLimits extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Limit n Project projectList =>
        plan select projectList limit n
    }
  }

  object ReduceLimits extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Limit n Limit m => Limit(plan, If(n < m, n, m))
    }
  }
}
