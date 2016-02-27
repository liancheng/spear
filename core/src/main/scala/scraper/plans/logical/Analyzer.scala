package scraper.plans.logical

import scraper.plans.logical.dsl._
import scraper.Catalog
import scraper.exceptions.{IllegalAggregationException, AnalysisException, ResolutionFailureException}
import scraper.expressions._
import scraper.plans.logical.Analyzer._
import scraper.plans.logical.patterns._
import scraper.trees.RulesExecutor.{FixedPoint, Once}
import scraper.trees.{Rule, RulesExecutor}

class Analyzer(catalog: Catalog) extends RulesExecutor[LogicalPlan] {
  private val resolutionBatch = RuleBatch("Resolution", FixedPoint.Unlimited, Seq(
    ExpandStars,
    new ResolveRelations(catalog),
    ResolveReferences,
    ResolveAggregates,
    DeduplicateReferences
  ))

  private val cleanupBatch = RuleBatch("Cleanup", Once, Seq(
    CleanupGeneratedOutput
  ))

  private val typeCheckBatch = RuleBatch("Type check", Once, Seq(
    TypeCheck
  ))

  override def batches: Seq[RuleBatch] = Seq(
    resolutionBatch,
    cleanupBatch,
    typeCheckBatch
  )

  override def apply(tree: LogicalPlan): LogicalPlan = {
    logTrace(
      s"""Analyzing logical query plan:
         |${tree.prettyTree}
         |""".stripMargin
    )
    super.apply(tree)
  }

  def resolve(tree: LogicalPlan): LogicalPlan = {
    logTrace(
      s"""Resolving logical query plan:
         |${tree.prettyTree}
         |""".stripMargin
    )
    apply(tree, resolutionBatch :: cleanupBatch :: Nil)
  }

  def typeCheck(tree: LogicalPlan): LogicalPlan = {
    if (tree.resolved) {
      logTrace(
        s"""Type checking logical query plan:
           |${tree.prettyTree}
           |""".stripMargin
      )
      apply(tree, typeCheckBatch :: Nil)
    } else {
      // Performs full analysis for unresolved logical query plan
      apply(tree)
    }
  }
}

object Analyzer {
  /**
   * This rule expands "`*`" appearing in `SELECT`.
   */
  object ExpandStars extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case Unresolved(Resolved(child) Project projectList) =>
        child select (projectList flatMap {
          case Star => child.output
          case e    => Seq(e)
        })
    }
  }

  /**
   * This rule tries to resolve [[scraper.expressions.UnresolvedAttribute UnresolvedAttribute]]s in
   * an logical plan operator using output [[scraper.expressions.Attribute Attribute]]s of its
   * children.
   */
  @throws[ResolutionFailureException](
    "If no candidate or multiple ambiguous candidate input attributes can be found"
  )
  object ResolveReferences extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case Unresolved(plan) if plan.childrenResolved =>
        plan transformExpressionsUp {
          case UnresolvedAttribute(name) =>
            def reportResolutionFailure(message: String): Nothing = {
              throw new ResolutionFailureException(
                s"""Failed to resolve attribute $name in logical query plan:
                   |${plan.prettyTree}
                   |$message
                   |""".stripMargin
              )
            }

            val candidates = plan.children flatMap (_.output) filter (_.name == name)

            candidates match {
              case Seq(attribute) =>
                attribute

              case Nil =>
                reportResolutionFailure("No candidate input attribute(s) found")

              case _ =>
                reportResolutionFailure {
                  val list = candidates map (_.debugString) mkString ", "
                  s"Multiple ambiguous input attributes found: $list"
                }
            }
        }
    }
  }

  /**
   * This rule resolves unresolved relations by looking up the table name from the `catalog`.
   */
  class ResolveRelations(catalog: Catalog) extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case UnresolvedRelation(name) => catalog lookupRelation name
    }
  }

  /**
   * This rule tries to transform all resolved logical plans operators (and expressions within them)
   * into strictly typed form.
   */
  @throws[AnalysisException]("If some resolved logical query plan operator doesn't type check")
  object TypeCheck extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case Resolved(plan) => plan.strictlyTypedForm.get
    }
  }

  /**
   * This rule eliminates all [[scraper.plans.logical.Subquery Subquery]] operators, since they are
   * only used to provide scoping information during analysis phase.
   */
  object EliminateSubqueries extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan Subquery _ => plan
    }
  }

  /**
   * This rule resolves ambiguous duplicated attributes/aliases introduced by binary logical query
   * plan operators like [[Join]] and [[SetOperator set operators]].  For example:
   * {{{
   *   // Self-join, equivalent to "SELECT * FROM t INNER JOIN t":
   *   val df = context table "t"
   *   val joined = df join df
   *
   *   // Self-union, equivalent to "SELECT 1 AS a UNION ALL SELECT 1 AS a":
   *   val df = context single (1 as 'a)
   *   val union = df union df
   * }}}
   */
  object DeduplicateReferences extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case join @ Join(Resolved(left), Resolved(right), _, _) if !join.duplicatesResolved =>
        val conflictingAttributes = left.outputSet & right.outputSet

        def hasDuplicates(attributes: Set[Attribute]): Boolean =
          (attributes & conflictingAttributes).nonEmpty

        val newLeft = left.collectFirst {
          // Handles relations that introduce ambiguous attributes
          case plan: MultiInstanceRelation if hasDuplicates(plan.outputSet) =>
            plan -> plan.newInstance()

          // Handles projections that introduce ambiguous aliases
          case plan @ Project(_, projectList) if hasDuplicates(collectAliases(projectList)) =>
            plan -> plan.copy(projectList = newAliases(projectList))
        } map {
          case (oldPlan, newPlan) =>
            val attributeRewrites = (oldPlan.output zip newPlan.output).toMap

            left transformDown {
              case plan if plan == oldPlan => newPlan
            } transformAllExpressions {
              case a: Attribute => attributeRewrites.getOrElse(a, a)
            }
        } getOrElse left

        join.copy(left = newLeft)
    }

    def collectAliases(projectList: Seq[NamedExpression]): Set[Attribute] =
      projectList.collect { case a: Alias => a.toAttribute }.toSet

    def newAliases(projectList: Seq[NamedExpression]): Seq[NamedExpression] = projectList map {
      case Alias(child, name, _) => child as name
      case e                     => e
    }
  }

  /**
   * This rule removes outermost generated output attributes by applying an optional projection.
   */
  object CleanupGeneratedOutput extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree match {
      case _ if tree.output exists isGenerated => tree select (tree.output filterNot isGenerated)
      case _                                   => tree
    }

    private def isGenerated(e: Expression): Boolean = e match {
      case _: GeneratedNamedExpression => true
      case _                           => false
    }
  }

  object ResolveAggregates extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case Resolved(plan @ Aggregate(child, groupingList, aggregateList)) =>
        val groupingSubstitutions = groupingList.map {
          a => a.child -> a.toAttribute
        }.toMap

        val substitutedAggs = aggregateList map {
          _.transformDown {
            case e => groupingSubstitutions.getOrElse(e, e)
          }.transformDown {
            case a: GroupingAttribute => a
            case a: AggregateFunction => a
            case a: Attribute         => throw new IllegalAggregationException(a, groupingList)
          }.asInstanceOf[NamedExpression]
        }

        plan.copy(aggregateList = substitutedAggs)
    }
  }
}
