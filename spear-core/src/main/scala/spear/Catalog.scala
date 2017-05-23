package spear

import scala.collection.mutable

import spear.exceptions.TableNotFoundException
import spear.plans.logical.LogicalPlan

trait Catalog {
  val functionRegistry: FunctionRegistry

  def registerRelation(tableName: Name, analyzedPlan: LogicalPlan): Unit

  def removeRelation(tableName: Name): Unit

  def lookupRelation(tableName: Name): LogicalPlan
}

class InMemoryCatalog extends Catalog {
  override val functionRegistry: FunctionRegistry = new InMemoryFunctionRegistry

  override def registerRelation(tableName: Name, analyzedPlan: LogicalPlan): Unit =
    tables(tableName) = analyzedPlan

  override def removeRelation(tableName: Name): Unit = tables -= tableName

  override def lookupRelation(tableName: Name): LogicalPlan =
    tables
      .get(tableName)
      .map { _ subquery tableName }
      .getOrElse { throw new TableNotFoundException(tableName) }

  private val tables: mutable.Map[Name, LogicalPlan] = mutable.Map.empty
}
