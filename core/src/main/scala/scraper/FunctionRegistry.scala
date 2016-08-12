package scraper

import scala.collection.mutable
import scala.language.existentials

import scraper.exceptions.FunctionNotFoundException
import scraper.expressions._

case class FunctionInfo(
  name: Name,
  builder: Seq[Expression] => Expression
)

object FunctionInfo {
  type FunctionBuilder = Seq[Expression] => Expression
}

trait FunctionRegistry {
  def registerFunction(fn: FunctionInfo): Unit

  def removeFunction(name: Name): Unit

  def lookupFunction(name: Name): FunctionInfo
}

class InMemoryFunctionRegistry extends FunctionRegistry {
  override def registerFunction(fn: FunctionInfo): Unit = map(fn.name) = fn

  override def removeFunction(name: Name): Unit = map.remove(name)

  override def lookupFunction(name: Name): FunctionInfo =
    map.getOrElse(name, throw new FunctionNotFoundException(name))

  private val map: mutable.Map[Name, FunctionInfo] = mutable.Map.empty[Name, FunctionInfo]
}
