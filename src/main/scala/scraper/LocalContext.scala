package scraper

import scala.reflect.runtime.universe.WeakTypeTag

import scraper.LocalContext.LocalQueryExecution
import scraper.plans.logical.LogicalPlan
import scraper.plans.physical.PhysicalPlan
import scraper.plans.{ Optimizer, QueryExecution, QueryPlanner, logical, physical }
import scraper.trees.RulesExecutor

trait Context {
  def execute(logicalPlan: LogicalPlan): QueryExecution
}

class LocalContext extends Context {
  def lift[T <: Product: WeakTypeTag](data: Traversable[T]): Dataset = {
    val queryExecution = new LocalQueryExecution(logical.LocalRelation(data), this)
    new Dataset(queryExecution)
  }

  def execute(logicalPlan: LogicalPlan): QueryExecution = new LocalQueryExecution(logicalPlan, this)
}

object LocalContext {
  class LocalQueryExecution(val logicalPlan: LogicalPlan, val context: LocalContext)
    extends QueryExecution {

    override protected def analyzer: RulesExecutor[LogicalPlan] = new Analyzer

    override protected def optimizer: RulesExecutor[LogicalPlan] = new Optimizer

    override protected def planner: QueryPlanner[LogicalPlan, PhysicalPlan] = new LocalQueryPlanner
  }

  class LocalQueryPlanner extends QueryPlanner[LogicalPlan, PhysicalPlan] {
    override def strategies: Seq[Strategy] = Seq(
      BasicOperators
    )

    object BasicOperators extends Strategy {
      override def apply(logicalPlan: LogicalPlan): Seq[PhysicalPlan] = logicalPlan match {
        case logical.Project(projectList, child) =>
          physical.Project(projectList, planLater(child)) :: Nil

        case logical.Filter(predicate, child) =>
          physical.Filter(predicate, planLater(child)) :: Nil

        case plan @ logical.LocalRelation(data, schema) =>
          physical.LocalRelation(data.toIterator, plan.output) :: Nil

        case _ => Nil
      }
    }
  }
}
