package scraper.plans.logical

import scraper.expressions.NamedExpression.named
import scraper.expressions._
import scraper.expressions.functions._

package object dsl {
  implicit class LogicalPlanDSL(plan: LogicalPlan) {
    def select(projectList: Seq[Expression]): Project =
      Project(plan, projectList map {
        // TODO Handle qualified star
        case UnresolvedAttribute("*", _) => Star
        case e: NamedExpression          => e
        case e                           => named(e)
      })

    def select(first: Expression, rest: Expression*): Project = select(first +: rest)

    def filter(condition: Expression): Filter = Filter(plan, condition)

    def where(condition: Expression): Filter = filter(condition)

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

    def groupBy(groupingList: Seq[Expression]): GroupedLogicalPlan =
      new GroupedLogicalPlan(plan, groupingList)

    def groupBy(first: Expression, rest: Expression*): GroupedLogicalPlan =
      new GroupedLogicalPlan(plan, first +: rest)
  }

  class GroupedLogicalPlan(plan: LogicalPlan, groupingList: Seq[Expression]) {
    def agg(aggregateList: Seq[NamedExpression]): Aggregate =
      Aggregate(plan, groupingList map (GroupingAlias(_)), aggregateList)

    def agg(first: NamedExpression, rest: NamedExpression*): Aggregate =
      Aggregate(plan, groupingList map (GroupingAlias(_)), first +: rest)
  }
}
