package scraper

import scala.collection.mutable

import scraper.Name.caseInsensitive
import scraper.exceptions.{FunctionNotFoundException, TableNotFoundException}
import scraper.expressions._
import scraper.plans.logical.LogicalPlan

trait Catalog {
  val functionRegistry: FunctionRegistry

  def registerRelation(tableName: Name, analyzedPlan: LogicalPlan): Unit

  def removeRelation(tableName: Name): Unit

  def lookupRelation(tableName: Name): LogicalPlan
}

class InMemoryCatalog extends Catalog {
  override val functionRegistry: FunctionRegistry = new FunctionRegistry {
    private val functions: mutable.Map[Name, FunctionInfo] =
      mutable.Map.empty[Name, FunctionInfo]

    override def lookupFunction(name: Name): FunctionInfo =
      functions.getOrElse(name, throw new FunctionNotFoundException(name))

    override def registerFunction(fn: FunctionInfo): Unit =
      functions(caseInsensitive(fn.name)) = fn

    override def removeFunction(name: Name): Unit = functions -= name
  }

  private val tables: mutable.Map[Name, LogicalPlan] = mutable.Map.empty

  functionRegistry.registerFunction(FunctionInfo(classOf[Rand], Rand))
  functionRegistry.registerFunction(FunctionInfo(classOf[Count], Count))
  functionRegistry.registerFunction(FunctionInfo(classOf[Sum], Sum))
  functionRegistry.registerFunction(FunctionInfo(classOf[Max], Max))
  functionRegistry.registerFunction(FunctionInfo(classOf[Min], Min))
  functionRegistry.registerFunction(FunctionInfo(classOf[BoolAnd], BoolAnd))
  functionRegistry.registerFunction(FunctionInfo(classOf[BoolOr], BoolOr))
  functionRegistry.registerFunction(FunctionInfo(classOf[Average], Average))
  functionRegistry.registerFunction(FunctionInfo(classOf[First], First))
  functionRegistry.registerFunction(FunctionInfo(classOf[Last], Last))

  override def registerRelation(tableName: Name, analyzedPlan: LogicalPlan): Unit =
    tables(tableName) = analyzedPlan

  override def removeRelation(tableName: Name): Unit = tables -= tableName

  override def lookupRelation(tableName: Name): LogicalPlan =
    tables
      .get(tableName)
      .map(_ subquery tableName)
      .getOrElse(throw new TableNotFoundException(tableName))
}
