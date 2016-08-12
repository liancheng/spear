package scraper

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import scraper.exceptions.{AnalysisException, TableNotFoundException}
import scraper.expressions._
import scraper.plans.logical.LogicalPlan

trait Catalog {
  val functionRegistry: FunctionRegistry

  def registerRelation(tableName: Name, analyzedPlan: LogicalPlan): Unit

  def removeRelation(tableName: Name): Unit

  def lookupRelation(tableName: Name): LogicalPlan
}

class InMemoryCatalog extends Catalog {
  override val functionRegistry: FunctionRegistry = new InMemoryFunctionRegistry

  private val builtInFunctions = Seq(
    function[Average](i"avg"),
    function[BoolAnd](i"bool_and"),
    function[BoolOr](i"bool_or"),
    function[Concat](i"concat"),
    function[Count](i"count"),
    function[First](i"avg"),
    function[Last](i"avg"),
    function[Max](i"max"),
    function[Min](i"min"),
    function[Product_](i"product"),
    function[Rand](i"rand"),
    function[RLike](i"rlike"),
    function[Sum](i"sum")
  )

  builtInFunctions foreach functionRegistry.registerFunction

  private def function[T <: Expression: ClassTag](name: Name): FunctionInfo = {
    val classTag = implicitly[ClassTag[T]]
    val builder = (args: Seq[Expression]) => {
      def tryVarargsConstructor: Try[Expression] = Try {
        classTag.runtimeClass.getDeclaredConstructor(classOf[Seq[_]])
      } map {
        _.newInstance(args) match { case fn: Expression => fn }
      }

      def tryNormalConstructor: Try[Expression] = Try {
        val argClasses = Seq.fill(args.length)(classOf[Expression])
        classTag.runtimeClass.getDeclaredConstructor(argClasses: _*)
      } map {
        _.newInstance(args: _*) match { case fn: Expression => fn }
      }

      tryVarargsConstructor orElse tryNormalConstructor match {
        case Success(fn)    => fn
        case Failure(cause) => throw new AnalysisException(cause.getMessage, cause)
      }
    }

    FunctionInfo(name, builder)
  }

  override def registerRelation(tableName: Name, analyzedPlan: LogicalPlan): Unit =
    tables(tableName) = analyzedPlan

  override def removeRelation(tableName: Name): Unit = tables -= tableName

  override def lookupRelation(tableName: Name): LogicalPlan =
    tables
      .get(tableName)
      .map(_ subquery tableName)
      .getOrElse(throw new TableNotFoundException(tableName))

  private val tables: mutable.Map[Name, LogicalPlan] = mutable.Map.empty
}
