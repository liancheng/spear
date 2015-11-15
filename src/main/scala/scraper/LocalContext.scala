package scraper

import scala.reflect.runtime.universe.WeakTypeTag

import scraper.LocalContext.{ LocalQueryPlanner, LocalQueryExecution }
import scraper.expressions.NamedExpression
import scraper.plans.logical.LogicalPlan
import scraper.plans.physical.PhysicalPlan
import scraper.plans.{ Optimizer, QueryExecution, QueryPlanner, logical, physical }
import scraper.trees.RulesExecutor

trait Context {
  private[scraper] def analyzer: RulesExecutor[LogicalPlan]

  private[scraper] def optimizer: RulesExecutor[LogicalPlan]

  private[scraper] def planner: QueryPlanner[LogicalPlan, PhysicalPlan]

  def execute(logicalPlan: LogicalPlan): QueryExecution

  def select(expressions: NamedExpression*): Dataset
}

class LocalContext extends Context {
  private[scraper] override val analyzer = new Analyzer

  private[scraper] override val optimizer = new Optimizer

  private[scraper] override val planner = new LocalQueryPlanner

  def lift[T <: Product: WeakTypeTag](data: Traversable[T]): Dataset = {
    val queryExecution = new LocalQueryExecution(logical.LocalRelation(data), this)
    new Dataset(queryExecution)
  }

  def execute(logicalPlan: LogicalPlan): QueryExecution = new LocalQueryExecution(logicalPlan, this)

  override def select(expressions: NamedExpression*): Dataset = {
    val queryExecution = new LocalQueryExecution(logical.SingleRowRelation, this)
    new Dataset(queryExecution)
  }
}

object LocalContext {
  class LocalQueryExecution(val logicalPlan: LogicalPlan, val context: LocalContext)
    extends QueryExecution

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
