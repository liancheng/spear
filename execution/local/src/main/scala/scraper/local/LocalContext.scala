package scraper.local

import scraper._
import scraper.config.Settings
import scraper.local.plans.physical
import scraper.local.plans.physical.HashAggregate
import scraper.local.plans.physical.dsl._
import scraper.parser.Parser
import scraper.plans.{QueryExecution, QueryPlanner}
import scraper.plans.logical._
import scraper.plans.physical.{NotImplemented, PhysicalPlan}

class LocalContext(val settings: Settings) extends Context {
  type QueryExecution = LocalQueryExecution

  override type Catalog = InMemoryCatalog

  override val catalog: Catalog = new Catalog

  override def parse(query: String): LogicalPlan = new Parser(settings).parse(query)

  private val analyzer = new Analyzer(catalog)

  override def analyze(plan: LogicalPlan): LogicalPlan = analyzer(plan)

  private val optimizer = new Optimizer

  override def optimize(plan: LogicalPlan): LogicalPlan = optimizer(plan)

  private val planner = new LocalQueryPlanner

  override def plan(plan: LogicalPlan): PhysicalPlan = planner(plan)

  def execute(logicalPlan: LogicalPlan): QueryExecution = new QueryExecution(logicalPlan, this)
}

class LocalQueryExecution(val logicalPlan: LogicalPlan, val context: Context)
  extends QueryExecution

class LocalQueryPlanner extends QueryPlanner[LogicalPlan, PhysicalPlan] {
  override def strategies: Seq[Strategy] = Seq(
    BasicOperators
  )

  object BasicOperators extends Strategy {
    override def apply(logicalPlan: LogicalPlan): Seq[PhysicalPlan] = logicalPlan match {
      case relation @ LocalRelation(data, _) =>
        physical.LocalRelation(data, relation.output) :: Nil

      case child Project projectList =>
        (planLater(child) select projectList) :: Nil

      case Aggregate(child, keys, functions) =>
        HashAggregate(planLater(child), keys, functions) :: Nil

      case child Filter condition =>
        (planLater(child) filter condition) :: Nil

      case child Limit n =>
        (planLater(child) limit n) :: Nil

      case Join(left, right, Inner, Some(condition)) =>
        (planLater(left) cartesian planLater(right) on condition) :: Nil

      case Join(left, right, Inner, _) =>
        (planLater(left) cartesian planLater(right)) :: Nil

      case child Sort order =>
        (planLater(child) orderBy order) :: Nil

      case child Subquery _ =>
        planLater(child) :: Nil

      case SingleRowRelation =>
        scraper.plans.physical.SingleRowRelation :: Nil

      case left Union right =>
        (planLater(left) union planLater(right)) :: Nil

      case left Intersect right =>
        (planLater(left) intersect planLater(right)) :: Nil

      case left Except right =>
        (planLater(left) except planLater(right)) :: Nil

      case plan =>
        NotImplemented(plan.nodeName, plan.children map planLater, plan.output) :: Nil
    }
  }
}
