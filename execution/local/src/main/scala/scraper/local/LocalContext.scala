package scraper.local

import scala.collection.{Iterable, mutable}
import scala.reflect.runtime.universe.WeakTypeTag

import scraper.config.Settings
import scraper.exceptions.TableNotFoundException
import scraper.local.plans.physical
import scraper.local.plans.physical.dsl._
import scraper.parser.Parser
import scraper.plans.logical._
import scraper.plans.physical.PhysicalPlan
import scraper.plans.{QueryExecution, QueryPlanner}
import scraper.types.{LongType, StructType}
import scraper._

class LocalContext(val settings: Settings) extends Context {
  type QueryExecution = LocalQueryExecution

  override type Catalog = LocalCatalog

  override val catalog: Catalog = new Catalog

  override def parse(query: String): LogicalPlan = new Parser(settings).parse(query)

  private val analyzer = new Analyzer(catalog)

  override def analyze(plan: LogicalPlan): LogicalPlan = analyzer(plan)

  private val optimizer = new Optimizer

  override def optimize(plan: LogicalPlan): LogicalPlan = optimizer(plan)

  private val planner = new LocalQueryPlanner

  override def plan(plan: LogicalPlan): PhysicalPlan = planner(plan)

  def lift[T <: Product: WeakTypeTag](data: Iterable[T]): DataFrame =
    new DataFrame(new QueryExecution(LocalRelation(data), this))

  def lift[T <: Product: WeakTypeTag](data: Iterable[T], columnNames: String*): DataFrame = {
    val LocalRelation(rows, output) = LocalRelation(data)
    val renamed = (StructType fromAttributes output rename columnNames).toAttributes
    new DataFrame(LocalRelation(rows, renamed), this)
  }

  def range(end: Long): DataFrame = range(0, end)

  def range(begin: Long, end: Long): DataFrame = {
    val rows = begin until end map (Row apply _)
    val output = StructType('id -> LongType.!).toAttributes
    new DataFrame(LocalRelation(rows, output), this)
  }

  override def q(query: String): DataFrame = new DataFrame(parse(query), this)

  def execute(logicalPlan: LogicalPlan): QueryExecution = new QueryExecution(logicalPlan, this)

  override def table(name: String): DataFrame = new DataFrame(catalog lookupRelation name, this)
}

class LocalCatalog extends Catalog {
  private val tables: mutable.Map[String, LogicalPlan] = mutable.Map.empty

  override def registerRelation(tableName: String, analyzedPlan: LogicalPlan): Unit =
    tables(tableName) = analyzedPlan

  override def removeRelation(tableName: String): Unit = tables -= tableName

  override def lookupRelation(tableName: String): LogicalPlan =
    tables
      .get(tableName)
      .map(_ subquery tableName)
      .getOrElse(throw new TableNotFoundException(tableName))
}

class LocalQueryExecution(val logicalPlan: LogicalPlan, val context: LocalContext)
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

      case _ => Nil
    }
  }
}
