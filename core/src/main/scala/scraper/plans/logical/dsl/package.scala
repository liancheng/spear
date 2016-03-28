package scraper.plans.logical

import scraper.expressions._
import scraper.expressions.AutoAlias.named
import scraper.expressions.functions._

package object dsl {
  implicit class LogicalPlanDSL(plan: LogicalPlan) {
    def select(projectList: Seq[Expression]): Project =
      Project(plan, projectList map {
        case UnresolvedAttribute("*", qualifier) => Star(qualifier)
        case e                                   => named(e)
      })

    def select(first: Expression, rest: Expression*): Project = select(first +: rest)

    def filter(condition: Expression): Filter = Filter(plan, condition)

    def filterOption(predicates: Seq[Expression]): LogicalPlan =
      predicates reduceOption And map filter getOrElse plan

    def where(condition: Expression): Filter = filter(condition)

    def having(condition: Expression): Filter = filter(condition)

    def limit(n: Expression): Limit = Limit(plan, n)

    def limit(n: Int): Limit = this limit lit(n)

    def orderBy(order: Seq[SortOrder]): Sort = Sort(plan, order)

    def orderBy(first: SortOrder, rest: SortOrder*): Sort = this orderBy (first +: rest)

    def distinct: Distinct = Distinct(plan)

    def subquery(name: String): Subquery = Subquery(plan, name)

    def subquery(name: Symbol): Subquery = plan subquery name.name

    def join(that: LogicalPlan): Join = Join(plan, that, Inner, None)

    def leftSemiJoin(that: LogicalPlan): Join = Join(plan, that, LeftSemi, None)

    def leftJoin(that: LogicalPlan): Join = Join(plan, that, LeftOuter, None)

    def rightJoin(that: LogicalPlan): Join = Join(plan, that, RightOuter, None)

    def outerJoin(that: LogicalPlan): Join = Join(plan, that, FullOuter, None)

    def union(that: LogicalPlan): Union = Union(plan, that)

    def intersect(that: LogicalPlan): Intersect = Intersect(plan, that)

    def except(that: LogicalPlan): Except = Except(plan, that)

    def groupBy(keys: Seq[Expression]): GroupedLogicalPlan = new GroupedLogicalPlan(plan, keys)

    def groupBy(first: Expression, rest: Expression*): GroupedLogicalPlan =
      new GroupedLogicalPlan(plan, first +: rest)

    def agg(projectList: Seq[Expression]): UnresolvedAggregate = this groupBy Nil agg projectList

    def agg(first: Expression, rest: Expression*): UnresolvedAggregate = agg(first +: rest)
  }

  class GroupedLogicalPlan(plan: LogicalPlan, keys: Seq[Expression]) {
    def agg(projectList: Seq[Expression]): UnresolvedAggregate =
      UnresolvedAggregate(plan, keys, projectList map named)

    def agg(first: Expression, rest: Expression*): UnresolvedAggregate = agg(first +: rest)
  }

  def table(name: String): UnresolvedRelation = UnresolvedRelation(name)

  def table(name: Symbol): UnresolvedRelation = table(name.name)

  def values(expressions: Seq[Expression]): Project = SingleRowRelation select expressions

  def values(first: Expression, rest: Expression*): Project = values(first +: rest)

  def let(cteRelations: Map[Symbol, LogicalPlan])(query: LogicalPlan): With =
    With(query, cteRelations map { case (name, plan) => name.name -> plan })

  def let(first: (Symbol, LogicalPlan), rest: (Symbol, LogicalPlan)*)(query: LogicalPlan): With =
    let((first +: rest).toMap)(query)
}
