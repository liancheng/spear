package scraper

import scala.reflect.runtime.universe.WeakTypeTag

import scraper.LocalContext.{ LocalQueryPlanner, LocalQueryExecution }
import scraper.expressions.NamedExpression
import scraper.expressions.dsl._
import scraper.plans.logical.{ Project, LogicalPlan }
import scraper.plans.physical.PhysicalPlan
import scraper.plans.{ Optimizer, QueryExecution, QueryPlanner, logical, physical }
import scraper.trees.RulesExecutor

trait Context {
  private[scraper] def analyzer: RulesExecutor[LogicalPlan]

  private[scraper] def optimizer: RulesExecutor[LogicalPlan]

  private[scraper] def planner: QueryPlanner[LogicalPlan, PhysicalPlan]

  def execute(logicalPlan: LogicalPlan): QueryExecution

  def select(expressions: NamedExpression*): DataFrame
}

class LocalContext extends Context {
  private[scraper] override val analyzer = new Analyzer

  private[scraper] override val optimizer = new Optimizer

  private[scraper] override val planner = new LocalQueryPlanner

  def lift[T <: Product: WeakTypeTag](data: Traversable[T]): DataFrame = {
    val queryExecution = new LocalQueryExecution(logical.LocalRelation(data), this)
    new DataFrame(queryExecution)
  }

  def lift[T <: Product: WeakTypeTag](data: Traversable[T], columnNames: String*): DataFrame =
    this lift data rename (columnNames: _*)

  def execute(logicalPlan: LogicalPlan): QueryExecution = new LocalQueryExecution(logicalPlan, this)

  override def select(expressions: NamedExpression*): DataFrame = {
    val project = logical.Project(expressions, logical.SingleRowRelation)
    val queryExecution = new LocalQueryExecution(project, this)
    new DataFrame(queryExecution)
  }

  def range(end: Long): DataFrame = range(0, end)

  def range(begin: Long, end: Long): DataFrame = {
    this lift (begin until end map Tuple1.apply) select ('_1 as 'id)
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

        case plan @ logical.SingleRowRelation =>
          physical.SingleRowRelation :: Nil

        case _ => Nil
      }
    }
  }
}
