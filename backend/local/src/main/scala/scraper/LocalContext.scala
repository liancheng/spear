package scraper

import scala.collection.{Iterable, mutable}
import scala.reflect.runtime.universe.WeakTypeTag

import scraper.config.Settings
import scraper.exceptions.TableNotFoundException
import scraper.parser.Parser
import scraper.plans.logical._
import scraper.plans.physical.PhysicalPlan
import scraper.plans.{QueryPlanner, physical}
import scraper.types.{LongType, StructType}

class LocalContext(val settings: Settings) extends Context {
  type QueryExecution = LocalQueryExecution

  override type Catalog = LocalCatalog

  override val catalog: Catalog = new Catalog

  override def parse(query: String): LogicalPlan = new Parser(settings).parse(query)

  private val analyzer = new Analyzer(catalog)

  override def analyze(plan: LogicalPlan): LogicalPlan = analyzer(plan)

  private val optimizer = new Optimizer

  override def optimize(plan: LogicalPlan): LogicalPlan = optimize(plan)

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
    val rows = (begin until end).map(Row.apply(_))
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
  extends plans.QueryExecution

class LocalQueryPlanner extends QueryPlanner[LogicalPlan, PhysicalPlan] {
  override def strategies: Seq[Strategy] = Seq(
    BasicOperators
  )

  object BasicOperators extends Strategy {
    override def apply(logicalPlan: LogicalPlan): Seq[PhysicalPlan] = logicalPlan match {
      case child Project projectList =>
        physical.Project(planLater(child), projectList) :: Nil

      case child Filter condition =>
        physical.Filter(planLater(child), condition) :: Nil

      case child Limit n =>
        physical.Limit(planLater(child), n) :: Nil

      case Join(left, right, Inner, maybeCondition) =>
        physical.CartesianProduct(planLater(left), planLater(right), maybeCondition) :: Nil

      case child Sort order =>
        physical.Sort(planLater(child), order) :: Nil

      case relation @ LocalRelation(data, _) =>
        physical.LocalRelation(data, relation.output) :: Nil

      case child Subquery _ =>
        planLater(child) :: Nil

      case SingleRowRelation =>
        physical.SingleRowRelation :: Nil

      case left Union right =>
        physical.Union(planLater(left), planLater(right)) :: Nil

      case left Intersect right =>
        physical.Intersect(planLater(left), planLater(right)) :: Nil

      case left Except right =>
        physical.Except(planLater(left), planLater(right)) :: Nil

      case _ => Nil
    }
  }
}
