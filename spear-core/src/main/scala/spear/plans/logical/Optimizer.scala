package spear.plans.logical

import spear.exceptions.LogicalPlanUnresolvedException
import spear.expressions._
import spear.expressions.InternalAlias.GroupingKeyNamespace
import spear.expressions.Literal.{False, True}
import spear.expressions.Predicate.{splitConjunction, toCNF}
import spear.trees.{FixedPoint, Rule, RuleGroup, Transformer}

class Optimizer extends Transformer(Optimizer.defaultRules) {
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
  val defaultRules: Seq[RuleGroup[LogicalPlan]] = Seq(
    RuleGroup("Optimizations", FixedPoint, Seq(
      CNFConversion,
      FoldConstant,
      FoldLogicalPredicate,
      EliminateConstantFilter,
      EliminateRedundantAlias,
      EliminateRedundantCast,

      FoldNegation,
      EliminateSubquery,
      EliminateRedundantLimits,
      MergeFilters,
      MergeProjects,

      PushFilterThroughProject,
      PushFilterThroughJoin,
      PushFilterThroughAggregate,
      PushProjectThroughLimit
    ))
  )
}

/**
 * This rule finds all foldable expressions and evaluates them into literals.
 */
object FoldConstant extends Rule[LogicalPlan] {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case node =>
      node transformExpressionsDown {
        case e if e.isFoldable => Literal(e.evaluated, e.dataType)
      }
  }
}

/**
 * This rule simplifies logical predicates containing `TRUE` and/or `FALSE`.
 */
object FoldLogicalPredicate extends Rule[LogicalPlan] {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case node =>
      node transformExpressionsDown {
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
}

/**
 * This rule reduces unnecessary `Not` operators.
 */
object FoldNegation extends Rule[LogicalPlan] {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case node =>
      node transformExpressionsDown {
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

        case !(IsNull(child))      => child.isNotNull
        case !(IsNotNull(child))   => child.isNull
      }
  }
}

/**
 * This rule reduces unnecessary casts. For example, implicit casts introduced by the analyzer may
 * produce redundant casts.
 */
object EliminateRedundantCast extends Rule[LogicalPlan] {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case node =>
      node transformExpressionsDown {
        case e Cast t if e.dataType == t                  => e
        case e Cast _ Cast t if e.dataType isCastableTo t => e cast t
      }
  }
}

/**
 * This rule merges adjacent projects. Aliases are also inlined/substituted when possible.
 */
object MergeProjects extends Rule[LogicalPlan] {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case plan Project projectList if projectList == plan.output =>
      plan

    case plan Project innerList Project outerList =>
      plan select (outerList map {
        case e: Alias        => e.copy(child = e.child unaliasUsing innerList)
        case e: AttributeRef => e unaliasUsing innerList as e.name withID e.expressionID
        case e               => e unaliasUsing innerList
      })
  }
}

/**
 * This rule reduces unnecessary aliases.
 */
object EliminateRedundantAlias extends Rule[LogicalPlan] {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case child Project projectList =>
      child select (projectList map eliminateNonTopLevelAliases)

    case Aggregate(child, keys, functions) =>
      child aggregate (keys, functions map eliminateNonTopLevelAliases)

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
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    // TODO CNF budget control
    case plan Filter condition => plan filter toCNF(condition)
  }
}

/**
 * This rule combines adjacent `Filter` operators into a single `Filter` operator.
 */
object MergeFilters extends Rule[LogicalPlan] {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case plan Filter inner Filter outer => plan filter inner && outer
  }
}

/**
 * This rule eliminates `Filter` operators with constant predicates.
 */
object EliminateConstantFilter extends Rule[LogicalPlan] {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case plan Filter True  => plan
    case plan Filter False => LocalRelation.empty(plan.output)
  }
}

/**
 * This rule pushes Filter operators beneath `Project` operators.
 */
object PushFilterThroughProject extends Rule[LogicalPlan] {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case plan Project projectList Filter condition if projectList forall { _.isPure } =>
      plan filter (condition unaliasUsing projectList) select projectList
  }
}

/**
 * This rule pushes `Filter` operators beneath `Join` operators whenever possible.
 */
object PushFilterThroughJoin extends Rule[LogicalPlan] {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case Join(lhs, rhs, Inner, joinCondition) Filter filterCondition =>
      val filterPredicates = splitConjunction(toCNF(filterCondition))

      val (lhsPushDown, rest) = filterPredicates partition {
        _.referenceSet subsetOfByID lhs.outputSet
      }

      val (rhsPushDown, stayUp) = rest partition {
        _.referenceSet subsetOfByID rhs.outputSet
      }

      if (lhsPushDown.nonEmpty) {
        val lhsList = lhsPushDown map { _.sqlLike } mkString ("[", ", ", "]")
        logDebug(s"Pushing predicates $lhsList through left join branch")
      }

      if (rhsPushDown.nonEmpty) {
        val rhsList = rhsPushDown map { _.sqlLike } mkString ("[", ", ", "]")
        logDebug(s"Pushing predicates $rhsList through right join branch")
      }

      lhs filter lhsPushDown join (rhs filter rhsPushDown) on stayUp ++ joinCondition
  }
}

object PushFilterThroughAggregate extends Rule[LogicalPlan] {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case Aggregate(child, keys, functions) Filter condition if functions forall { _.isPure } =>
      // Only predicates referencing no aggregate functions can be pushed down.
      val (stayUp, pushDown) = splitConjunction(toCNF(condition)) partition containsAggregation

      if (pushDown.nonEmpty) {
        logDebug {
          val pushDownList = pushDown map { _.sqlLike } mkString ("[", ", ", "]")
          s"Pushing down predicates $pushDownList through aggregate"
        }
      }

      val unaliasedPushDown = pushDown map { _ unaliasUsing (keys, GroupingKeyNamespace) }

      child filter unaliasedPushDown aggregate (keys, functions) filter stayUp
  }

  private def containsAggregation(expression: Expression): Boolean = expression.collectFirstDown {
    case e: NamedExpression if e.namespace == InternalAlias.AggregateFunctionNamespace =>
  }.nonEmpty
}

object PushProjectThroughLimit extends Rule[LogicalPlan] {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case plan Limit n Project projectList if projectList.length < plan.output.length =>
      plan select projectList limit n
  }
}

object EliminateRedundantLimits extends Rule[LogicalPlan] {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case plan Limit n Limit m => plan limit Least(n, m)
  }
}

/**
 * This rule eliminates all `Subquery` operators, since they are only useful for providing scoping
 * information during analysis phase.
 */
object EliminateSubquery extends Rule[LogicalPlan] {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case child Subquery _ => child
  } transformDown {
    case node =>
      node transformExpressionsDown {
        case ref: AttributeRef => ref.copy(qualifier = None)
      }
  }
}
