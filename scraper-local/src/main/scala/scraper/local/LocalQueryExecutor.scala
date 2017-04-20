package scraper.local

import scraper._
import scraper.local.plans.physical
import scraper.local.plans.physical.HashAggregate
import scraper.local.plans.physical.dsl._
import scraper.parsers.QueryExpressionParser.queryExpression
import scraper.plans.QueryPlanner
import scraper.plans.logical._
import scraper.plans.logical.analysis.Analyzer
import scraper.plans.physical.{NotImplemented, PhysicalPlan}

class LocalQueryExecutor extends QueryExecutor {
  override val catalog: Catalog = new InMemoryCatalog

  override def parse(query: String): LogicalPlan = queryExpression.parse(query).get.value

  override def analyze(plan: LogicalPlan): LogicalPlan = analyzer apply plan

  override def optimize(plan: LogicalPlan): LogicalPlan = optimizer apply plan

  override def plan(plan: LogicalPlan): PhysicalPlan = planner apply plan

  private val analyzer = new Analyzer(catalog)

  private val optimizer = new Optimizer

  private val planner = new LocalQueryPlanner
}

class LocalQueryPlanner extends QueryPlanner[LogicalPlan, PhysicalPlan] {
  override def strategies: Seq[Strategy] = Seq(
    BasicOperators
  )

  object BasicOperators extends Strategy {
    override def apply(logicalPlan: LogicalPlan): Seq[PhysicalPlan] = logicalPlan match {
      case relation @ LocalRelation(data, _) =>
        physical.LocalRelation(data, relation.output) :: Nil

      case Project(projectList, child) =>
        (planLater(child) select projectList) :: Nil

      case Aggregate(keys, functions, child) =>
        HashAggregate(planLater(child), keys, functions) :: Nil

      case Filter(condition, child) =>
        (planLater(child) filter condition) :: Nil

      case Limit(n, child) =>
        (planLater(child) limit n) :: Nil

      case Join(Inner, Some(condition), left, right) =>
        (planLater(left) cartesianJoin planLater(right) on condition) :: Nil

      case Join(Inner, _, left, right) =>
        (planLater(left) cartesianJoin planLater(right)) :: Nil

      case Sort(order, child) =>
        (planLater(child) orderBy order) :: Nil

      case Subquery(_, child) =>
        planLater(child) :: Nil

      case _: SingleRowRelation =>
        scraper.plans.physical.SingleRowRelation :: Nil

      case left Union right =>
        (planLater(left) union planLater(right)) :: Nil

      case left Intersect right =>
        (planLater(left) intersect planLater(right)) :: Nil

      case left Except right =>
        (planLater(left) except planLater(right)) :: Nil

      case plan =>
        NotImplemented(plan.nodeName.toString, plan.children map planLater, plan.output) :: Nil
    }
  }
}
