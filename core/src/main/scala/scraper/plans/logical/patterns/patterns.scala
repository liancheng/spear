package scraper.plans.logical

import scraper.expressions
import scraper.expressions.Predicate._
import scraper.expressions._

package object patterns {
  /**
   * A pattern that matches any number of altering pure project or filter operators on top of
   * another relational operator, extracting top level projections, predicate conditions of all
   * filter operators, and the relational operator underneath. [[Alias Aliases]] are inlined when
   * possible.
   *
   * @note This pattern is only available for resolved logical plans.
   */
  private[scraper] object PhysicalOperation {
    private type Result = (Seq[NamedExpression], Seq[Expression], LogicalPlan)

    private type IntermediateResult = (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan)

    def unapply(plan: LogicalPlan): Option[Result] = {
      require(plan.isResolved, {
        val patternName = this.getClass.getSimpleName stripSuffix "$"
        s"Pattern $patternName is only available for resolved logical plans."
      })

      val (maybeProjectList, predicates, child) = collectProjectsAndFilters(plan)
      Some(maybeProjectList getOrElse plan.output, predicates, child)
    }

    private def collectProjectsAndFilters(plan: LogicalPlan): IntermediateResult = plan match {
      case unary: UnaryLogicalPlan =>
        val (maybeChildProjectList, predicates, grandChild) = collectProjectsAndFilters(unary.child)
        val aliases = collectAliases(maybeChildProjectList.toSeq.flatten)

        plan match {
          case _ Project projectList if projectList forall (_.isPure) =>
            (Some(projectList map (inlineAliases(aliases, _))), predicates, grandChild)

          case _ Filter condition if condition.isPure =>
            val reducedCondition = inlineAliases(aliases, condition)
            (maybeChildProjectList, predicates ++ splitConjunction(reducedCondition), grandChild)

          case other =>
            (None, Nil, other)
        }

      case other =>
        (None, Nil, other)
    }

    /**
     * Inlines all known `aliases` appearing in `expression`.
     *
     * @param aliases A map from known aliases to corresponding aliased expressions.
     * @param expression The target expression.
     */
    def inlineAliases[T <: Expression](aliases: Map[Attribute, Expression], expression: T): T =
      expression.transformUp {
        // Removes redundant aliases. E.g., it helps to reduce
        //
        //   SELECT a1 AS a2 FROM (
        //     SELECT e AS a1 FROM t
        //   )
        //
        // to
        //
        //   SELECT e AS a2 FROM t
        case a @ Alias(ref: AttributeRef, _, _) if aliases contains ref =>
          a.copy(child = aliases(ref))

        // Alias inlining. E.g., it helps to transform the following plan tree
        //
        //   Filter a1 > 10
        //    Project (f1 + 1) AS a1
        //     Relation f1:INT!
        //
        // to
        //
        //   Filter ((f1 + 1) AS a1) > 10
        //    Project (f1 + 1) AS a1
        //     Relation f1:INT!
        //
        // so that we can push down the predicate:
        //
        //   Project (f1 + 1) AS a1
        //    Filter (f1 + 1) AS a1 > 10
        //     Relation f1:INT!
        case ref: AttributeRef if aliases contains ref =>
          Alias(aliases(ref), ref.name, ref.expressionID)
      }.asInstanceOf[T]

    /**
     * Inlines all aliases found in `input` expression that are defined in `fields`.
     */
    def inlineAliases[T <: Expression](fields: Seq[NamedExpression], input: T): T =
      inlineAliases(collectAliases(fields), input)

    /**
     * Builds a map from all aliases found in `projectList` to corresponding aliased expressions
     */
    def collectAliases(projectList: Seq[NamedExpression]): Map[Attribute, Expression] =
      projectList.collect { case a: Alias => a.toAttribute -> a.child }.toMap
  }

  /** A simple pattern that matches resolved [[LogicalPlan]]s and [[expressions.Expression]]s */
  object Resolved {
    def unapply(plan: LogicalPlan): Option[LogicalPlan] = Some(plan) filter (_.isResolved)

    def unapply(expression: Expression): Option[Expression] = Some(expression) filter (_.isResolved)
  }

  /** A simple pattern that matches unresolved [[LogicalPlan]]s and [[expressions.Expression]]s */
  object Unresolved {
    def unapply(plan: LogicalPlan): Option[LogicalPlan] = Some(plan) filter (!_.isResolved)

    def unapply(expression: Expression): Option[Expression] =
      Some(expression) filter (!_.isResolved)
  }
}
