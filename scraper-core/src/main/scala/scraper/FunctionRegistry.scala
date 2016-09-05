package scraper

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import scraper.exceptions.{FunctionInstantiationException, FunctionNotFoundException}
import scraper.expressions._
import scraper.expressions.aggregates._

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

  override def removeFunction(name: Name): Unit = map remove name

  override def lookupFunction(name: Name): FunctionInfo =
    map.getOrElse(name, throw new FunctionNotFoundException(name))

  private val map: mutable.Map[Name, FunctionInfo] = mutable.Map.empty[Name, FunctionInfo]

  Seq(
    function[Coalesce](i"coalesce"),
    function[Rand](i"rand"),
    function[CollectList](i"collect_list"),
    function[CollectSet](i"collect_set"),

    function[Count](i"count"),
    function[First](i"first"),
    function[First](i"first_value"),
    function[Last](i"last"),
    function[Last](i"last_value"),
    function[Max](i"max"),
    function[Min](i"min"),
    function[Average](i"average"),
    function[Average](i"avg"),
    function[Sum](i"sum"),
    function[Product_](i"product"),
    function[BoolAnd](i"bool_and"),
    function[BoolOr](i"bool_or"),

    function[Concat](i"concat"),

    function[CreateNamedStruct](i"named_struct"),
    function[CreateArray](i"array"),
    function[CreateMap](i"map")
  ) foreach registerFunction

  private def function[T <: Expression: ClassTag](name: Name): FunctionInfo = {
    val classTag = implicitly[ClassTag[T]]

    def instantiateWithVararg(args: Seq[Expression]): Any = {
      val argClasses = classOf[Seq[Expression]]
      val constructor = classTag.runtimeClass.getDeclaredConstructor(argClasses)
      constructor.newInstance(args)
    }

    def instantiate(args: Seq[Expression]): Any = {
      val argClasses = Seq.fill(args.length)(classOf[Expression])
      val constructor = classTag.runtimeClass.getDeclaredConstructor(argClasses: _*)
      constructor.newInstance(args: _*)
    }

    val builder = (args: Seq[Expression]) => {
      Try(instantiateWithVararg(args)) orElse Try(instantiate(args)) match {
        case Success(fn: Expression) => fn
        case Failure(cause)          => throw new FunctionInstantiationException(name, cause)
        case _                       => throw new FunctionInstantiationException(name)
      }
    }

    FunctionInfo(name, builder)
  }
}
