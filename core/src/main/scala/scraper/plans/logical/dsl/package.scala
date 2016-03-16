package scraper.plans.logical

import scraper.expressions.AutoAlias.named
import scraper.expressions._
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

    def where(condition: Expression): Filter = filter(condition)

    def having(condition: Expression): Filter = filter(condition)

    def limit(n: Expression): Limit = Limit(plan, n)

    def limit(n: Int): Limit = this limit lit(n)

    def orderBy(order: Seq[SortOrder]): Sort = Sort(plan, order)

    def orderBy(first: SortOrder, rest: SortOrder*): Sort = this orderBy (first +: rest)

    def distinct: Distinct = Distinct(plan)

    def subquery(name: String): Subquery = Subquery(plan, name)

    def subquery(name: Symbol): Subquery = plan subquery name.name

    def as(name: String): Subquery = plan subquery name

    def as(name: Symbol): Subquery = plan as name.name

    def join(that: LogicalPlan): Join = Join(plan, that, Inner, None)

    def leftJoin(that: LogicalPlan): Join = Join(plan, that, LeftOuter, None)

    def rightJoin(that: LogicalPlan): Join = Join(plan, that, RightOuter, None)

    def outerJoin(that: LogicalPlan): Join = Join(plan, that, FullOuter, None)

    def union(that: LogicalPlan): Union = Union(plan, that)

    def intersect(that: LogicalPlan): Intersect = Intersect(plan, that)

    def except(that: LogicalPlan): Except = Except(plan, that)

    def groupBy(keys: Seq[Expression]): GroupedLogicalPlan = new GroupedLogicalPlan(plan, keys)

    def groupBy(first: Expression, rest: Expression*): GroupedLogicalPlan =
      new GroupedLogicalPlan(plan, first +: rest)

    def agg(projectList: Seq[Expression]): RichAggregate = this groupBy Nil agg projectList

    def agg(first: Expression, rest: Expression*): RichAggregate = agg(first +: rest)
  }

  class GroupedLogicalPlan(plan: LogicalPlan, keys: Seq[Expression]) {
    def agg(projectList: Seq[Expression]): RichAggregate =
      RichAggregate(plan, keys, projectList map named)

    def agg(first: Expression, rest: Expression*): RichAggregate = agg(first +: rest)
  }
}
