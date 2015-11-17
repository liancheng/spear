package scraper.plans

import scraper.expressions.Predicate.splitConjunction
import scraper.expressions._
import scraper.plans.logical.{ UnaryLogicalPlan, Filter, LogicalPlan, Project }

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
    private type Result = (Seq[NamedExpression], Seq[Predicate], LogicalPlan)

    private type AliasMap = Map[Attribute, Expression]

    private type IntermediateResult = (Option[Seq[NamedExpression]], Seq[Predicate], LogicalPlan)

    def unapply(plan: LogicalPlan): Option[Result] = {
      assert(plan.resolved, {
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
          case Project(projectList, _) =>
            val reducedProjectList = projectList map reduceAliases[NamedExpression](aliases)
            (Some(reducedProjectList), predicates, grandChild)

          case Filter(condition, _) =>
            val reducedCondition = reduceAliases[Predicate](aliases)(condition)
            (maybeChildProjectList, predicates ++ splitConjunction(reducedCondition), grandChild)

          case other =>
            (None, Nil, other)
        }

      case other =>
        (None, Nil, other)
    }

    /**
     * Finds reducible [[Alias aliases]] appeared in `expressions`, and inlines/substitutes them.
     *
     * @param aliases A map from all known aliases to corresponding aliased expressions.
     * @param expression The target expression.
     */
    private def reduceAliases[T <: Expression](aliases: AliasMap)(expression: Expression): T =
      expression.transformUp {
        // Alias substitution. E.g., it reduces
        //
        //   SELECT a1 AS a2 FROM (
        //     SELECT e AS a1 FROM t
        //   )
        //
        // to
        //
        //   SELECT e AS a2 FROM t
        case a @ Alias(_, ref: AttributeRef, _) if aliases contains ref =>
          a.copy(child = aliases(ref))

        // Alias inlining. E.g., it reduces
        //
        //   SELECT a1 FROM (
        //     SELECT e AS a1 FROM t
        //   )
        //
        // to
        //
        //   SELECT e AS a1 FROM t
        case ref @ AttributeRef(name, _, _, id) if aliases contains ref =>
          Alias(name, aliases(ref), id)
      }.asInstanceOf[T]

    private def collectAliases(projectList: Seq[NamedExpression]): AliasMap =
      projectList.collect { case a: Alias => a.toAttribute -> a.child }.toMap
  }
}
