package scraper

import scala.collection.mutable
import scala.language.existentials

import scraper.exceptions.{FunctionNotFoundException, TableNotFoundException}
import scraper.expressions._
import scraper.plans.logical.LogicalPlan
import scraper.plans.logical.dsl._

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

  override def registerRelation(tableName: String, analyzedPlan: LogicalPlan): Unit =
    tables(tableName) = analyzedPlan

  override def removeRelation(tableName: String): Unit = tables -= tableName

  override def lookupRelation(tableName: String): LogicalPlan =
    tables
      .get(tableName)
      .map(_ subquery tableName)
      .getOrElse(throw new TableNotFoundException(tableName))
}
