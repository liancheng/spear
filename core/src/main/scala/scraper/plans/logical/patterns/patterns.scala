package scraper.plans.logical

import scraper.expressions
import scraper.expressions.Predicate._
import scraper.expressions._

package object patterns {
  /**
   * A pattern that matches any number of altering project or filter operators on top of another
   * relational operator, extracting top level projections, predicate conditions of all filter
   * operators, and the relational operator underneath. [[Alias Aliases]] are inline-ed/substituted
   * when possible.
   *
   * @note This pattern is only available for resolved logical plans.
   */
  private[scraper] object PhysicalOperation {
    private type Result = (Seq[NamedExpression], Seq[Expression], LogicalPlan)

    private type IntermediateResult = (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan)

    def unapply(plan: LogicalPlan): Option[Result] = {
      require(plan.resolved, {
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
          case _ Project projectList =>
            (Some(projectList map (reduceAliases(aliases, _))), predicates, grandChild)

          case _ Filter condition =>
            val reducedCondition = reduceAliases(aliases, condition)
            (maybeChildProjectList, predicates ++ splitConjunction(reducedCondition), grandChild)

          case other =>
            (None, Nil, other)
        }

      case other =>
        (None, Nil, other)
    }

    /**
     * Finds reducible [[Alias]]es and [[AttributeRef]]s referring to [[Alias]]s appearing in
     * `expressions`, then inlines/substitutes them.
     *
     * @param aliases A map from all known aliases to corresponding aliased expressions.
     * @param expression The target expression.
     */
    def reduceAliases[T <: Expression](aliases: Map[Attribute, Expression], expression: T): T =
      expression.transformUp {
        // Alias substitution. E.g., it helps to reduce
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

        // Alias inlining. E.g., it helps to reduce
        //
        //   SELECT a1 FROM (
        //     SELECT e AS a1 FROM t
        //   )
        //
        // to
        //
        //   SELECT e AS a1 FROM t
        case ref @ AttributeRef(name, _, _, id) if aliases contains ref =>
          Alias(aliases(ref), name, id)
      }.asInstanceOf[T]

    def collectAliases(projectList: Seq[NamedExpression]): Map[Attribute, Expression] =
      projectList.collect { case a: Alias => a.toAttribute -> a.child }.toMap
  }

  /** A simple pattern that matches resolved [[LogicalPlan]]s and [[expressions.Expression]]s */
  object Resolved {
    def unapply(plan: LogicalPlan): Option[LogicalPlan] = Some(plan) filter (_.resolved)

    def unapply(expression: Expression): Option[Expression] = Some(expression) filter (_.resolved)
  }

  /** A simple pattern that matches unresolved [[LogicalPlan]]s and [[expressions.Expression]]s */
  object Unresolved {
    def unapply(plan: LogicalPlan): Option[LogicalPlan] = Some(plan) filter (!_.resolved)

    def unapply(expression: Expression): Option[Expression] = Some(expression) filter (!_.resolved)
  }
}
