package scraper.plans.logical

import scraper.exceptions.LogicalPlanUnresolvedException
import scraper.expressions._
import scraper.expressions.InternalNamedExpression.{ForAggregation, ForGrouping}
import scraper.expressions.Literal.{False, True}
import scraper.expressions.Predicate.{splitConjunction, toCNF}
import scraper.plans.logical.Optimizer._
import scraper.trees.{Rule, RulesExecutor}
import scraper.trees.RulesExecutor.FixedPoint

class Optimizer extends RulesExecutor[LogicalPlan] {
  override def batches: Seq[RuleBatch] = Seq(
    RuleBatch("Optimizations", FixedPoint.Unlimited, Seq(
      FoldConstants,
      FoldLogicalPredicates,
      EliminateConstantFilters,
      CNFConversion,

      ReduceAliases,
      ReduceCasts,
      MergeFilters,
      ReduceLimits,
      ReduceNegations,
      MergeProjects,
      EliminateSubqueries,

      PushFiltersThroughProjects,
      PushFiltersThroughJoins,
      PushFiltersThroughAggregates,
      PushProjectsThroughLimits
    ))
  )

  override def apply(tree: LogicalPlan): LogicalPlan = {
    if (!tree.isResolved) {
      throw new LogicalPlanUnresolvedException(tree)
    }

    logDebug(
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
   * This rule finds all foldable expressions and evaluates them into literals.
   */
  object FoldConstants extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressionsDown {
      case e if e.isFoldable => Literal(e.evaluated, e.dataType)
    }
  }

  /**
   * This rule simplifies logical predicates containing `TRUE` and/or `FALSE`.
   */
  object FoldLogicalPredicates extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressionsDown {
      case True || _          => True
      case _ || True          => True

      case False && _         => False
      case _ && False         => False

      case !(True)            => False
      case !(False)           => True

      case a && b if a same b => a
      case a || b if a same b => a

      case If(True, yes, _)   => yes
      case If(False, _, no)   => no
    }
  }

  /**
   * This rule reduces unnecessary `Not` operators.
   */
  object ReduceNegations extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressionsDown {
      case !(True)               => False
      case !(False)              => True

      case !(!(child))           => child
      case !(lhs === rhs)        => lhs =/= rhs
      case !(lhs =/= rhs)        => lhs === rhs

      case !(lhs > rhs)          => lhs <= rhs
      case !(lhs >= rhs)         => lhs < rhs
      case !(lhs < rhs)          => lhs >= rhs
      case !(lhs <= rhs)         => lhs > rhs

      case If(!(c), t, f)        => If(c, f, t)

      case !(a && b)             => !a || !b
      case !(a || b)             => !a && !b

      case a && !(b) if a same b => False
      case a || !(b) if a same b => True

      case !(a) && b if a same b => False
      case !(a) || b if a same b => True

      case !(IsNull(child))      => IsNotNull(child)
      case !(IsNotNull(child))   => IsNull(child)
    }
  }

  /**
   * This rule reduces unnecessary casts. For example, implicit casts introduced by the analyzer may
   * produce redundant casts.
   */
  object ReduceCasts extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressionsDown {
      case e Cast t if e.dataType == t                  => e
      case e Cast _ Cast t if e.dataType isCastableTo t => e cast t
    }
  }

  /**
   * This rule merges adjacent projects. Aliases are also inlined/substituted when possible.
   */
  object MergeProjects extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Project projectList if projectList == plan.output =>
        plan

      case plan Project innerList Project outerList =>
        val unalias = Alias.unaliasUsing[Expression](innerList) _

        plan select (outerList map {
          case a: Alias        => a.copy(child = unalias(a.child))
          case a: AttributeRef => unalias(a) as a.name withID a.expressionID
          case e               => unalias(e)
        })
    }
  }

  /**
   * This rule reduces unnecessary aliases.
   */
  object ReduceAliases extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case child Project projectList =>
        child select (projectList map eliminateNonTopLevelAliases)

      case Aggregate(child, keys, functions) =>
        child resolvedGroupBy keys agg (functions map eliminateNonTopLevelAliases)

      case plan =>
        plan transformExpressionsUp { case a: Alias => a.child }
    }

    private def eliminateNonTopLevelAliases[T <: NamedExpression](expression: T): T =
      expression.transformChildrenUp {
        case a: Alias         => a.child
        case a: InternalAlias => a.child
      }.asInstanceOf[T]
  }

  /**
   * This rule converts a predicate to CNF (Conjunctive Normal Form).
   *
   * Since we don't support existential/universal quantifiers or implications, this rule simply
   * pushes negations inwards by applying De Morgan's law and distributes `Or`s inwards over `And`s.
   *
   * @see https://en.wikipedia.org/wiki/Conjunctive_normal_form
   */
  object CNFConversion extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      // TODO CNF budget control
      case plan Filter condition => plan filter toCNF(condition)
    }
  }

  /**
   * This rule combines adjacent `Filter` operators into a single `Filter` operator.
   */
  object MergeFilters extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Filter inner Filter outer => plan filter (inner && outer)
    }
  }

  /**
   * This rule eliminates `Filter` operators with constant predicates.
   */
  object EliminateConstantFilters extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Filter True  => plan
      case plan Filter False => LocalRelation(Nil, plan.output)
    }
  }

  /**
   * This rule pushes Filter operators beneath `Project` operators.
   */
  object PushFiltersThroughProjects extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Project projectList Filter condition if projectList forall (_.isPure) =>
        val rewrittenCondition = Alias.unaliasUsing(projectList)(condition)
        plan filter rewrittenCondition select projectList
    }
  }

  /**
   * This rule pushes `Filter` operators beneath `Join` operators whenever possible.
   */
  object PushFiltersThroughJoins extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case (join @ Join(left, right, Inner, joinCondition)) Filter filterCondition =>
        val (leftPredicates, rightPredicates, commonPredicates) =
          partitionByReferencedBranches(splitConjunction(toCNF(filterCondition)), left, right)

        if (leftPredicates.nonEmpty) {
          logDebug {
            val leftList = leftPredicates map (_.sqlLike) mkString ("[", ", ", "]")
            s"Pushing predicates $leftList through left join branch"
          }
        }

        if (rightPredicates.nonEmpty) {
          logDebug {
            val rightList = rightPredicates map (_.sqlLike) mkString ("[", ", ", "]")
            s"Pushing predicates $rightList through right join branch"
          }
        }

        left
          .filterOption(leftPredicates)
          .join(right.filterOption(rightPredicates))
          .onOption(commonPredicates ++ joinCondition)
    }

    private def partitionByReferencedBranches(
      predicates: Seq[Expression],
      left: LogicalPlan,
      right: LogicalPlan
    ): (Seq[Expression], Seq[Expression], Seq[Expression]) = {
      val (leftPredicates, rest) = predicates partition {
        _.referenceSet subsetOfByID left.outputSet
      }

      val (rightPredicates, commonPredicates) = rest partition {
        _.referenceSet subsetOfByID right.outputSet
      }

      (leftPredicates, rightPredicates, commonPredicates)
    }
  }

  object PushFiltersThroughAggregates extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case Aggregate(child, keys, functions) Filter condition if functions forall (_.isPure) =>
        // Predicates that don't reference any aggregate functions can be pushed down
        val (stayUp, pushDown) = splitConjunction(toCNF(condition)) partition containsAggregation

        if (pushDown.nonEmpty) {
          logDebug({
            val pushDownList = pushDown map (_.sqlLike) mkString ("[", ", ", "]")
            s"Pushing down predicates $pushDownList through aggregate"
          })
        }

        val unaliasedPushDown = pushDown map InternalAlias.unaliasUsing(keys, ForGrouping)

        child
          .filterOption(unaliasedPushDown)
          .resolvedGroupBy(keys)
          .agg(functions)
          .filterOption(stayUp)
    }

    private def containsAggregation(expression: Expression): Boolean = expression.collectFirst {
      case e: InternalNamedExpression if e.purpose == ForAggregation =>
    }.nonEmpty
  }

  object PushProjectsThroughLimits extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Limit n Project projectList if projectList.length < plan.output.length =>
        plan select projectList limit n
    }
  }

  object ReduceLimits extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Limit n Limit m => plan limit Least(n, m)
    }
  }

  /**
   * This rule eliminates all `Subquery` operators, since they are only useful for providing scoping
   * information during analysis phase.
   */
  object EliminateSubqueries extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case child Subquery _ => child
    } transformAllExpressionsDown {
      case ref: AttributeRef => ref.copy(qualifier = None)
    }
  }
}
