package scraper

import scala.language.existentials

import scraper.expressions._

case class FunctionInfo(
  name: String,
  functionClass: Class[_ <: Expression],
  builder: Seq[Expression] => Expression
)

object FunctionInfo {
  type FunctionBuilder = Seq[Expression] => Expression

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

  def removeFunction(name: Name): Unit

  def lookupFunction(name: Name): FunctionInfo
}
