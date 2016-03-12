package scraper.plans.logical

import scraper.Catalog
import scraper.exceptions.{AnalysisException, IllegalAggregationException, ResolutionFailureException}
import scraper.expressions.NamedExpression.{AnonymousColumnName, UnquotedAttribute}
import scraper.expressions.ResolvedAttribute.intersectByID
import scraper.expressions._
import scraper.plans.logical.Analyzer._
import scraper.plans.logical.dsl._
import scraper.plans.logical.patterns._
import scraper.trees.RulesExecutor.{FixedPoint, Once}
import scraper.trees.{Rule, RulesExecutor}

class Analyzer(catalog: Catalog) extends RulesExecutor[LogicalPlan] {
  private val expressionResolutionBatch =
    RuleBatch("Expression resolution", FixedPoint.Unlimited, Seq(
      new ResolveRelations(catalog),
      ExpandStars,
      ResolveReferences,
      DeduplicateReferences,
      ResolveAliases
    ))

  private val planResolutionBatch =
    RuleBatch("Plan resolution", FixedPoint.Unlimited, Seq(
      ResolveAggregates
    ))

  private val cleanupBatch =
    RuleBatch("Cleanup", Once, Seq(
      CleanupGeneratedOutput
    ))

  private val typeCheckBatch =
    RuleBatch("Type check", Once, Seq(
      TypeCheck
    ))

  override def batches: Seq[RuleBatch] = Seq(
    expressionResolutionBatch,
    planResolutionBatch,
    cleanupBatch,
    typeCheckBatch
  )

  override def apply(tree: LogicalPlan): LogicalPlan = {
    logDebug(
      s"""Analyzing logical query plan:
         |
         |${tree.prettyTree}
         |""".stripMargin
    )
    super.apply(tree)
  }

  def resolve(tree: LogicalPlan): LogicalPlan = {
    logDebug(
      s"""Resolving logical query plan:
         |
         |${tree.prettyTree}
         |""".stripMargin
    )
    apply(tree, expressionResolutionBatch :: planResolutionBatch :: cleanupBatch :: Nil)
  }

  def typeCheck(tree: LogicalPlan): LogicalPlan = {
    if (tree.isResolved) {
      logDebug(
        s"""Type checking logical query plan:
           |
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
      case Unresolved(plan) if plan.isDeduplicated =>
        resolveReferences(plan)
    }

    def resolveReferences(plan: LogicalPlan): LogicalPlan = plan transformExpressionsUp {
      case UnresolvedAttribute(name, qualifier) =>
        def reportResolutionFailure(message: String): Nothing = {
          throw new ResolutionFailureException(
            s"""Failed to resolve attribute $name in logical query plan:
               |${plan.prettyTree}
               |$message
               |""".stripMargin
          )
        }

        val candidates = plan.children flatMap (_.output) filter {
          case a: AttributeRef => a.name == name && (qualifier.toSet subsetOf a.qualifier.toSet)
          case _               => false
        }

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
      case Resolved(plan) => plan.strictlyTyped.get
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
      case plan if plan.isChildrenResolved && !plan.isDeduplicated =>
        plan match {
          case node: Join      => node.copy(right = deduplicateRight(node.left, node.right))
          case node: Union     => node.copy(right = deduplicateRight(node.left, node.right))
          case node: Intersect => node.copy(right = deduplicateRight(node.left, node.right))
          case node: Except    => node.copy(right = deduplicateRight(node.left, node.right))
        }
    }

    def deduplicateRight(left: LogicalPlan, right: LogicalPlan): LogicalPlan = {
      val conflictingAttributes = intersectByID(left.outputSet, right.outputSet)

      def hasDuplicates(attributes: Set[Attribute]): Boolean =
        intersectByID(attributes, conflictingAttributes).nonEmpty

      right collectFirst {
        // Handles relations that introduce ambiguous attributes
        case plan: MultiInstanceRelation if hasDuplicates(plan.outputSet) =>
          plan -> plan.newInstance()

        // Handles projections that introduce ambiguous aliases
        case plan @ Project(_, projectList) if hasDuplicates(collectAliases(projectList)) =>
          plan -> plan.copy(projectList = newAliases(projectList))
      } map {
        case (oldPlan, newPlan) =>
          val attributeRewrites = (oldPlan.output map (_.expressionID) zip newPlan.output).toMap

          right transformDown {
            case plan if plan == oldPlan => newPlan
          } transformAllExpressions {
            case a: Attribute => attributeRewrites getOrElse (a.expressionID, a)
          }
      } getOrElse right
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
      case Resolved(child Project projectList) if projectList exists containsAggregation =>
        child groupBy Nil agg projectList

      case UnresolvedAggregate(Resolved(child), groupingList, projectList) =>
        // Aliases all found aggregate functions with `AggregationAlias` and builds rewriting map.
        val aggs = projectList.flatMap(_ collect { case a: AggregateFunction => a }).distinct
        val aggAliases = aggs map AggregationAlias.apply
        val aggRewrites = (aggs, aggAliases).zipped.map((_: Expression) -> _.toAttribute).toMap

        // Aliases all grouping expressions with `GroupingAlias` and builds rewriting map.
        val groupingAliases = groupingList map GroupingAlias.apply
        val groupingRewrites =
          (groupingList, groupingAliases).zipped.map((_: Expression) -> _.toAttribute).toMap

        // Replaces all occurrences of aggregate functions and grouping expressions with
        // corresponding generated attributes.
        val rewrittenProjectList = projectList map {
          _ transformDown {
            case e => aggRewrites orElse groupingRewrites applyOrElse (e, identity[Expression])
          }
        }

        // Reports invalid aggregation expressions if any.  Project list of an `UnresolvedAggregate`
        // should only consist of aggregation functions, grouping expressions, and literals.  After
        // rewriting aggregation functions and grouping expressions to `AggregationAttribute`s and
        // `GroupingAttribute`s, no `AttributeRef`s should exist in the rewritten project list.
        rewrittenProjectList filter {
          _.collectFirst { case _: AttributeRef => () }.nonEmpty
        } foreach {
          e => throw new IllegalAggregationException(e, groupingAliases)
        }

        Aggregate(child, groupingAliases, aggAliases) select rewrittenProjectList
    }

    def containsAggregation(e: NamedExpression): Boolean =
      e.collectFirst { case _: AggregateFunction => () }.nonEmpty
  }

  object ResolveAliases extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressions {
      case UnresolvedAlias(Resolved(child: NamedExpression)) =>
        child

      case UnresolvedAlias(Resolved(child: Expression)) =>
        // Uses `UnquotedAttribute` to eliminate back-ticks and in generated alias names.
        // TODO Also replaces literal strings to `UnquotedAttributes` to eliminate double-quotes
        def rewrite(e: Expression): Expression = e.transformDown {
          case a: Attribute => UnquotedAttribute(a)
        }
        child as (rewrite(child).sql getOrElse AnonymousColumnName)
    }
  }
}
