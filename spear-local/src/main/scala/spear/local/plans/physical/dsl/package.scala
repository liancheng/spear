package spear.local.plans.physical

import spear.expressions._
import spear.expressions.functions._
import spear.plans.physical.PhysicalPlan

package object dsl {
  implicit class PhysicalPlanDSL(plan: PhysicalPlan) {
    def select(projectList: Seq[NamedExpression]): Project = Project(plan, projectList)

    def select(first: NamedExpression, rest: NamedExpression*): Project = select(first +: rest)

    def filter(condition: Expression): Filter = Filter(plan, condition)

    def limit(n: Expression): Limit = Limit(plan, n)

    def limit(n: Int): Limit = limit(lit(n))

    def orderBy(order: Seq[SortOrder]): Sort = Sort(plan, order)

    def orderBy(first: SortOrder, rest: SortOrder*): Sort = plan orderBy (first +: rest)

    def cartesianJoin(that: PhysicalPlan): CartesianJoin = CartesianJoin(plan, that, None)

    def union(that: PhysicalPlan): Union = Union(plan, that)

    def intersect(that: PhysicalPlan): Intersect = Intersect(plan, that)

    def except(that: PhysicalPlan): Except = Except(plan, that)
  }
}
