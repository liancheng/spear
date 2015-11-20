package scraper.plans.logical

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
    private type Result = (Seq[NamedExpression], Seq[Predicate], LogicalPlan)

    private type AliasMap = Map[Attribute, Expression]

    private type IntermediateResult = (Option[Seq[NamedExpression]], Seq[Predicate], LogicalPlan)

    def unapply(plan: LogicalPlan): Option[Result] = {
      assert(plan.resolved, {
        val patternName = this.getClass.getSimpleName stripSuffix "$"
        s"Pattern $patternName is only available for resolved logical plans."
      })

      val (maybeProjections, predicates, child) = collectProjectsAndFilters(plan)
      Some(maybeProjections getOrElse plan.output, predicates, child)
    }

    private def collectProjectsAndFilters(plan: LogicalPlan): IntermediateResult = plan match {
      case unary: UnaryLogicalPlan =>
        val (maybeChildProjections, predicates, grandChild) = collectProjectsAndFilters(unary.child)
        val aliases = collectAliases(maybeChildProjections.toSeq.flatten)

        plan match {
          case _ Project projections =>
            val aliasesReducedProjections = projections map (reduceAliases(aliases, _))
            (Some(aliasesReducedProjections), predicates, grandChild)

          case _ Filter condition =>
            val reducedCondition = reduceAliases(aliases, condition)
            (maybeChildProjections, predicates ++ splitConjunction(reducedCondition), grandChild)

          case other =>
            (None, Nil, other)
        }

      case other =>
        (None, Nil, other)
    }

    /**
     * Finds reducible [[Alias]]es appeared in `expressions`, and inlines/substitutes them.
     *
     * @param aliases A map from all known aliases to corresponding aliased expressions.
     * @param expression The target expression.
     */
    def reduceAliases[T <: Expression](aliases: AliasMap, expression: T): T =
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

    def collectAliases(projectList: Seq[NamedExpression]): AliasMap =
      projectList.collect { case a: Alias => a.toAttribute -> a.child }.toMap
  }
}
