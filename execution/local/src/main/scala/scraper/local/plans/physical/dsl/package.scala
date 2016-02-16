package scraper.local.plans.physical

import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.physical.PhysicalPlan

package object dsl {
  implicit class PhysicalPlanDsl(plan: PhysicalPlan) {
    def select(projectList: Seq[Expression]): Project =
      Project(plan, projectList.zipWithIndex map {
        case (UnresolvedAttribute("*"), _) => Star
        case (e: NamedExpression, _)       => e
        case (e, ordinal)                  => e as (e.sql getOrElse s"col$ordinal")
      })

    def select(first: Expression, rest: Expression*): Project = select(first +: rest)

    def filter(condition: Expression): Filter = Filter(plan, condition)

    def where(condition: Expression): Filter = filter(condition)

    def limit(n: Expression): Limit = Limit(plan, n)

    def limit(n: Int): Limit = limit(lit(n))

    def orderBy(order: Seq[SortOrder]): Sort = Sort(plan, order)

    def orderBy(first: SortOrder, rest: SortOrder*): Sort = plan orderBy (first +: rest)

    def cartesian(that: PhysicalPlan): CartesianProduct = CartesianProduct(plan, that, None)

    def union(that: PhysicalPlan): Union = Union(plan, that)

    def intersect(that: PhysicalPlan): Intersect = Intersect(plan, that)

    def except(that: PhysicalPlan): Except = Except(plan, that)
  }
}
