package scraper.plans.logical

import scraper._
import scraper.exceptions.{AnalysisException, IllegalAggregationException, ResolutionFailureException}
import scraper.expressions._
import scraper.expressions.AutoAlias.AnonymousColumnName
import scraper.expressions.NamedExpression.{newExpressionID, UnquotedName}
import scraper.expressions.dsl._
import scraper.plans.logical.patterns._
import scraper.trees.{Rule, RulesExecutor}
import scraper.trees.RulesExecutor.{FixedPoint, Once}
import scraper.types.StringType

class Analyzer(catalog: Catalog) extends RulesExecutor[LogicalPlan] {
  override def batches: Seq[RuleBatch] = Seq(
    RuleBatch("Substitution", FixedPoint.Unlimited, Seq(
      InlineCTERelationsAsSubqueries
    )),

    RuleBatch("Resolution", FixedPoint.Unlimited, Seq(
      new ResolveRelations(catalog),
      new ResolveFunctions(catalog),
      ExpandStars,
      ResolveReferences,
      ResolveAliases,
      DeduplicateReferences,

      ResolveSortReferences,
      RewriteDistinctsAsAggregates,
      GlobalAggregates,
      MergeHavingConditions,
      MergeSortsOverAggregates,
      ResolveAggregates
    )),

    RuleBatch("Type check", Once, Seq(
      TypeCheck
    )),

    RuleBatch("Post-analysis check", Once, Seq(
      PostAnalysisCheck
    ))
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

  /**
   * This rule inlines CTE relation definitions as sub-queries.
   */
  object InlineCTERelationsAsSubqueries extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case plan @ With(_, _, _: With) =>
        plan

      case With(child, name, plan) =>
        child transformDown {
          case UnresolvedRelation(`name`) => plan subquery name
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
   * This rule expands "`*`" appearing in `SELECT`.
   */
  object ExpandStars extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case Unresolved(Resolved(child) Project projectList) =>
        child select (projectList flatMap {
          case Star(qualifier) => expand(qualifier, child.output)
          case e               => Seq(e)
        })
    }

    private def expand(maybeQualifier: Option[Name], input: Seq[Attribute]): Seq[Attribute] =
      maybeQualifier map { qualifier =>
        input collect {
          case ref: AttributeRef if ref.qualifier contains qualifier => ref
        }
      } getOrElse input
  }

  /**
   * This rule tries to resolve [[scraper.expressions.UnresolvedAttribute UnresolvedAttribute]]s in
   * an logical plan operator using output [[scraper.expressions.Attribute Attribute]]s of its
   * children.
   *
   * @throws scraper.exceptions.ResolutionFailureException If no candidate or multiple ambiguous
   *         candidate input attributes can be found.
   */
  object ResolveReferences extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case Unresolved(plan) if plan.isDeduplicated =>
        resolveReferences(plan)
    }

    private def resolveReferences(plan: LogicalPlan): LogicalPlan = plan transformExpressionsUp {
      case unresolved @ UnresolvedAttribute(name, qualifier) =>
        def reportResolutionFailure(message: String): Nothing = {
          throw new ResolutionFailureException(
            s"""Failed to resolve attribute ${unresolved.debugString} in logical query plan:
               |${plan.prettyTree}
               |$message
               |""".stripMargin
          )
        }

        val candidates = plan.children flatMap (_.output) collect {
          case a: AttributeRef if a.name == name && qualifier == a.qualifier => a
          case a: AttributeRef if a.name == name && qualifier.isEmpty        => a
        }

        candidates match {
          case Seq(attribute) =>
            attribute

          case Nil =>
            // We don't report resolution failure here since the attribute might be resolved later
            // with the help of other analysis rules.
            unresolved

          case _ =>
            reportResolutionFailure {
              val list = candidates map (_.debugString) mkString ", "
              s"Multiple ambiguous input attributes found: $list"
            }
        }
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
      case plan if plan.children.forall(_.isResolved) && !plan.isDeduplicated =>
        plan match {
          case node: Join      => node.copy(right = deduplicateRight(node.left, node.right))
          case node: Union     => node.copy(right = deduplicateRight(node.left, node.right))
          case node: Intersect => node.copy(right = deduplicateRight(node.left, node.right))
          case node: Except    => node.copy(right = deduplicateRight(node.left, node.right))
        }
    }

    def deduplicateRight(left: LogicalPlan, right: LogicalPlan): LogicalPlan = {
      val conflictingAttributes = left.outputSet intersectByID right.outputSet

      def hasDuplicates(attributes: Set[Attribute]): Boolean =
        (attributes intersectByID conflictingAttributes).nonEmpty

      right collectFirst {
        // Handles relations that introduce ambiguous attributes
        case plan: MultiInstanceRelation if hasDuplicates(plan.outputSet) =>
          plan -> plan.newInstance()

        // Handles projections that introduce ambiguous aliases
        case plan @ Project(_, projectList) if hasDuplicates(collectAliases(projectList)) =>
          plan -> plan.copy(projectList = projectList map {
            case a: Alias => a withID newExpressionID()
            case e        => e
          })
      } map {
        case (oldPlan, newPlan) =>
          val rewrite = (
            oldPlan.output map (_.expressionID) zip (newPlan.output map (_.expressionID))
          ).toMap

          right transformDown {
            case plan if plan == oldPlan => newPlan
          } transformAllExpressions {
            case a: AttributeRef => rewrite get a.expressionID map a.withID getOrElse a
          }
      } getOrElse right
    }

    private def collectAliases(projectList: Seq[NamedExpression]): Set[Attribute] =
      projectList.collect { case a: Alias => a.toAttribute }.toSet
  }

  /**
   * This rule converts [[scraper.expressions.AutoAlias AutoAlias]]es into real
   * [[scraper.expressions.Alias Alias]]es, as long as aliased expressions are resolved.
   */
  object ResolveAliases extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressions {
      case AutoAlias(Resolved(child: Expression)) =>
        // Uses `UnquotedName` to eliminate back-ticks and double-quotes in generated alias names.
        val alias = child.transformDown {
          case a: AttributeRef                  => UnquotedName(a)
          case Literal(lit: String, StringType) => UnquotedName(lit)
        }.sql getOrElse AnonymousColumnName

        child as Name.caseInsensitive(alias)
    }
  }

  /**
   * This rule resolves [[scraper.expressions.UnresolvedFunction unresolved functions]] by looking
   * up function names from the [[Catalog]].
   */
  class ResolveFunctions(catalog: Catalog) extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressions {
      case UnresolvedFunction(name, (_: Star) :: Nil, false) if name == i"count" =>
        Count(1)

      case Count((_: Star)) =>
        Count(1)

      case UnresolvedFunction(_, (_: Star) :: Nil, true) =>
        throw new AnalysisException("DISTINCT cannot be used together with star")

      case UnresolvedFunction(name, (_: Star) :: Nil, _) =>
        throw new AnalysisException("Only COUNT function may have star as argument")

      case UnresolvedFunction(name, args, distinct) if args forall (_.isResolved) =>
        val fnInfo = catalog.functionRegistry.lookupFunction(name)
        fnInfo.builder(args) match {
          case f: AggregateFunction if distinct =>
            DistinctAggregateFunction(f)

          case f if distinct =>
            throw new AnalysisException(s"Function $name is not an aggregate function")

          case f =>
            f
        }
    }
  }

  object RewriteDistinctsAsAggregates extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case Distinct(Resolved(child)) =>
        child groupBy child.output agg child.output
    }
  }

  object ResolveSortReferences extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      // Ignores global aggregates
      case plan @ (Resolved(_ Project projectList) Sort _) if containsAggregation(projectList) =>
        plan

      case Unresolved(plan @ Resolved(child Project projectList) Sort order) =>
        val orderReferences = order.flatMap(_.collect { case a: Attribute => a }).distinct
        child select (projectList ++ orderReferences).distinct orderBy order select plan.output
    }
  }

  /**
   * This rule converts [[Project]]s containing aggregate functions into unresolved global
   * aggregates, i.e., an [[UnresolvedAggregate]] without grouping keys.
   */
  object GlobalAggregates extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case Resolved(child Project projectList) if containsAggregation(projectList) =>
        child groupBy Nil agg projectList
    }
  }

  /**
   * A [[Filter]] directly over an [[UnresolvedAggregate]] corresponds to a "having condition" in
   * SQL.  Having condition can only reference grouping keys and aggregated expressions, and thus
   * must be resolved together with the [[UnresolvedAggregate]] beneath it.  This rule merges such
   * having conditions into [[UnresolvedAggregate]]s beneath them so that they can be resolved
   * together later.
   */
  object MergeHavingConditions extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case (agg: UnresolvedAggregate) Filter condition =>
        // All having conditions should be preserved
        agg.copy(havingConditions = agg.havingConditions :+ condition)
    }
  }

  /**
   * A [[Sort]] directly over an [[UnresolvedAggregate]] is special.  Its ordering expressions can
   * only reference grouping keys and aggregated expressions, and thus must be resolved together
   * with the [[UnresolvedAggregate]] beneath it.  This rule merges such [[Sort]]s into
   * [[UnresolvedAggregate]]s beneath them so that they can be resolved together later.
   */
  object MergeSortsOverAggregates extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformDown {
      case (agg: UnresolvedAggregate) Sort order =>
        // Only preserves the last sort order
        agg.copy(order = order)
    }
  }

  /**
   * This rule resolves [[UnresolvedAggregate]]s into an [[Aggregate]], an optional [[Filter]] if
   * there exists a having condition, an optional [[Sort]] if there exist any sort ordering
   * expressions, and a top-level [[Project]].
   */
  object ResolveAggregates extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan =
      tree transformDown (skip orElse resolveUnresolvedAggregate)

    private val skip: PartialFunction[LogicalPlan, LogicalPlan] = {
      // Waits until all adjacent having conditions are merged
      case plan @ ((_: UnresolvedAggregate) Filter _) =>
        plan

      // Waits until all adjacent sorts are merged
      case plan @ ((_: UnresolvedAggregate) Sort _) =>
        plan

      // Waits until projected list, having condition, and sort order expressions are all resolved
      case plan: UnresolvedAggregate if plan.expressions exists (!_.isResolved) =>
        plan
    }

    private val resolveUnresolvedAggregate: PartialFunction[LogicalPlan, LogicalPlan] = {
      case agg @ UnresolvedAggregate(Resolved(child), keys, projectList, conditions, order) =>
        // Aliases all grouping keys
        val keyAliases = keys map (GroupingAlias(_))
        val rewriteKeys = keys.zip(keyAliases.map(_.toAttribute)).toMap

        // Collects all aggregate functions and checks for nested ones (which are illegal).
        val aggs = collectAggregateFunctions(projectList ++ conditions ++ order)

        // Checks for invalid expressions like `MAX(COUNT(*))`.
        aggs foreach checkNestedAggregateFunction

        // Aliases all found aggregate functions
        val aggAliases = aggs map (AggregationAlias(_))
        val rewriteAggs = (aggs: Seq[Expression]).zip(aggAliases.map(_.toAttribute)).toMap

        def rewrite(expression: Expression) = expression transformDown {
          case e => rewriteKeys orElse rewriteAggs applyOrElse (e, identity[Expression])
        }

        // Replaces grouping keys and aggregate functions in having condition, sort ordering
        // expressions, and projected named expressions.
        val newConditions = conditions map rewrite
        val newOrdering = order map (order => order.copy(child = rewrite(order.child)))
        val newProjectList = projectList map (e => rewrite(e) -> e) map {
          // Top level `GeneratedAttribute`s should be aliased to names of the original expressions
          case (g: GeneratedAttribute, e) => g as e.name
          case (e, _)                     => e
        }

        checkAggregation("SELECT field", keys, newProjectList)
        checkAggregation("HAVING condition", keys, newConditions)
        checkAggregation("ORDER BY expression", keys, newOrdering)

        child
          .resolvedGroupBy(keyAliases)
          .agg(aggAliases)
          .filterOption(newConditions)
          .orderByOption(newOrdering)
          .select(newProjectList)
    }

    private def collectAggregateFunctions(expressions: Seq[Expression]): Seq[AggregateFunction] = {
      // `DistinctAggregateFunction`s must be collected first. Otherwise, their child expressions,
      // which are also `AggregateFunction`s, will also be collected unexpectedly.
      val distinctAggs = expressions.flatMap(_ collect {
        case a: DistinctAggregateFunction => a
      }).distinct

      val aggs = expressions.map(_ transformDown {
        // Eliminates previously collected `DistinctAggregateFunction`s first
        case AggregationAlias(a: DistinctAggregateFunction, _) => AggregationAlias(a).toAttribute
      }).flatMap(_ collect {
        case a: AggregateFunction => a
      }).distinct

      distinctAggs ++ aggs
    }

    private def checkNestedAggregateFunction(agg: AggregateFunction): Unit =
      agg.children.foreach(_ collectFirst {
        case nested: AggregateFunction =>
          throw new IllegalAggregationException(agg, nested)
      })

    private def checkAggregation(
      part: String, keys: Seq[Expression], expressions: Seq[Expression]
    ): Unit = expressions foreach { e =>
      e.references collectFirst {
        case a: AttributeRef => a
      } foreach { a =>
        throw new IllegalAggregationException(part, a, e, keys)
      }
    }
  }

  /**
   * This rule tries to transform all resolved logical plans operators (and expressions within them)
   * into strictly-typed form.
   *
   * @throws scraper.exceptions.AnalysisException If some resolved logical query plan operator
   *         doesn't type check.
   */
  object TypeCheck extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
      case Resolved(plan) => plan.strictlyTyped.get
    }
  }

  object PostAnalysisCheck extends Rule[LogicalPlan] {
    override def apply(tree: LogicalPlan): LogicalPlan = {
      ensureResolved(tree)
      ensureNoGeneratedOutputAttributes(tree)
      tree
    }

    private def ensureResolved(tree: LogicalPlan): Unit = if (!tree.isResolved) {
      // Tries to collect a "minimum" unresolved logical plan node.
      tree.collectFirst {
        case Unresolved(plan) if plan.children.forall(_.isResolved) =>
          // Tries to collect a "minimum" unresolved expression.
          plan.collectFirstFromAllExpressions {
            case Unresolved(e) if e.children.forall(_.isResolved) =>
              throw new ResolutionFailureException(
                s"""Expression ${e.debugString} in the following logical plan is not fully resolved:
                   |
                   |${plan.prettyTree}
                   |""".stripMargin
              )
          }

          // For unresolved logical plan nodes whose expressions are all resolved (e.g.,
          // `UnresolvedAggregate`), simply report the logical plan.
          throw new ResolutionFailureException(
            s"""The following logical plan is not fully resolved:
               |
               |${tree.prettyTree}
               |""".stripMargin
          )
      }
    }

    private def ensureNoGeneratedOutputAttributes(tree: LogicalPlan): Unit = {
      val generated = tree.output.collect { case e: GeneratedNamedExpression => e }

      if (generated.nonEmpty) {
        val generatedList = generated mkString ("[", ", ", "]")
        throw new ResolutionFailureException(
          s"""Found generated output attributes $generatedList in logical plan:
             |
             |${tree.prettyTree}
             |
             |Generated named expressions are reserved for internal use during analysis phase.
             |""".stripMargin
        )
      }
    }
  }

  private def containsAggregation(expressions: Seq[Expression]): Boolean =
    expressions exists (_.collectFirst { case _: AggregateFunction => () }.nonEmpty)
}
