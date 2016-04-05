package scraper

import scala.collection.{mutable, Iterable}
import scala.language.existentials
import scala.reflect.runtime.universe.WeakTypeTag

import scraper.config.Settings
import scraper.exceptions.{FunctionNotFoundException, TableNotFoundException}
import scraper.expressions._
import scraper.plans.logical.{LocalRelation, LogicalPlan, SingleRowRelation}
import scraper.plans.logical.dsl._
import scraper.plans.physical.PhysicalPlan
import scraper.types.{LongType, StructType}

case class FunctionInfo(
  name: String,
  functionClass: Class[_ <: Expression],
  builder: Seq[Expression] => Expression
)

object FunctionInfo {
  def apply[F <: Expression](
    functionClass: Class[F],
    builder: Expression => Expression
  ): FunctionInfo = {
    val name = (functionClass.getSimpleName stripSuffix "$").toUpperCase
    FunctionInfo(name, functionClass, (args: Seq[Expression]) => builder(args.head))
  }
}

trait FunctionRegistry {
  def registerFunction(fn: FunctionInfo): Unit

  def removeFunction(name: String): Unit

  def lookupFunction(name: String): FunctionInfo
}

trait Catalog {
  val functionRegistry: FunctionRegistry

  def registerRelation(tableName: String, analyzedPlan: LogicalPlan): Unit

  def removeRelation(tableName: String): Unit

  def lookupRelation(tableName: String): LogicalPlan
}

class InMemoryCatalog extends Catalog {
  override val functionRegistry: FunctionRegistry = new FunctionRegistry {
    private val functions: mutable.Map[String, FunctionInfo] =
      mutable.Map.empty[String, FunctionInfo]

    override def lookupFunction(name: String): FunctionInfo =
      functions.getOrElse(name.toLowerCase, throw new FunctionNotFoundException(name))

    override def registerFunction(fn: FunctionInfo): Unit = functions(fn.name.toLowerCase) = fn

    override def removeFunction(name: String): Unit = functions -= name
  }

  private val tables: mutable.Map[String, LogicalPlan] = mutable.Map.empty

  functionRegistry.registerFunction(FunctionInfo(classOf[Count], Count))
  functionRegistry.registerFunction(FunctionInfo(classOf[Sum], Sum))
  functionRegistry.registerFunction(FunctionInfo(classOf[Max], Max))
  functionRegistry.registerFunction(FunctionInfo(classOf[Min], Min))
  functionRegistry.registerFunction(FunctionInfo(classOf[BoolAnd], BoolAnd))
  functionRegistry.registerFunction(FunctionInfo(classOf[BoolOr], BoolOr))
  functionRegistry.registerFunction(FunctionInfo(classOf[Average], Average))

  override def registerRelation(tableName: String, analyzedPlan: LogicalPlan): Unit =
    tables(tableName) = analyzedPlan

  override def removeRelation(tableName: String): Unit = tables -= tableName

  override def lookupRelation(tableName: String): LogicalPlan =
    tables
      .get(tableName)
      .map(_ subquery tableName)
      .getOrElse(throw new TableNotFoundException(tableName))
}

trait Context {
  type QueryExecution <: plans.QueryExecution

  type Catalog <: scraper.Catalog

  def settings: Settings

  def catalog: Catalog

  /**
   * Parses given query string to a [[scraper.plans.logical.LogicalPlan logical plan]].
   */
  def parse(query: String): LogicalPlan

  /**
   * Analyzes an unresolved [[scraper.plans.logical.LogicalPlan logical plan]] and outputs its
   * strictly-typed version.
   */
  def analyze(plan: LogicalPlan): LogicalPlan

  /**
   * Optimizes a resolved [[scraper.plans.logical.LogicalPlan logical plan]] into another equivalent
   * but more performant version.
   */
  def optimize(plan: LogicalPlan): LogicalPlan

  /**
   * Plans a [[scraper.plans.logical.LogicalPlan logical plan]] into an executable
   * [[scraper.plans.physical.PhysicalPlan physical plan]].
   */
  def plan(plan: LogicalPlan): PhysicalPlan

  def execute(logicalPlan: LogicalPlan): QueryExecution

  private lazy val values: DataFrame = new DataFrame(SingleRowRelation, this)

  def values(first: Expression, rest: Expression*): DataFrame = values select first +: rest

  def q(query: String): DataFrame = new DataFrame(parse(query), this)

  def table(name: String): DataFrame = new DataFrame(catalog lookupRelation name, this)

  def table(name: Symbol): DataFrame = table(name.name)

  def lift[T <: Product: WeakTypeTag](data: Iterable[T]): DataFrame =
    new DataFrame(LocalRelation(data), this)

  def lift[T <: Product: WeakTypeTag](first: T, rest: T*): DataFrame = lift(first +: rest)

  def lift[T <: Product: WeakTypeTag](data: Iterable[T], columnNames: String*): DataFrame = {
    val LocalRelation(rows, output) = LocalRelation(data)
    val renamed = (StructType fromAttributes output rename columnNames).toAttributes
    new DataFrame(LocalRelation(rows, renamed), this)
  }

  def range(end: Long): DataFrame = range(0, end)

  def range(begin: Long, end: Long): DataFrame = range(begin, end, 1L)

  def range(begin: Long, end: Long, step: Long): DataFrame = {
    val rows = begin until end by step map (Row apply _)
    val output = StructType('id -> LongType.!).toAttributes
    new DataFrame(LocalRelation(rows, output), this)
  }
}

object Context {
  implicit class QueryString(query: String)(implicit context: Context) {
    def q: DataFrame = context q query
  }
}
