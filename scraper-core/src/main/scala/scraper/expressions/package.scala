package scraper

import scala.language.implicitConversions

import scraper.expressions.typecheck._
import scraper.parser.Parser
import scraper.types._

package object expressions {
  implicit def `Boolean->Literal`(value: Boolean): Literal = Literal(value, BooleanType)

  implicit def `Byte->Literal`(value: Byte): Literal = Literal(value, ByteType)

  implicit def `Short->Literal`(value: Short): Literal = Literal(value, ShortType)

  implicit def `Int->Literal`(value: Int): Literal = Literal(value, IntType)

  implicit def `Long->Literal`(value: Long): Literal = Literal(value, LongType)

  implicit def `Float->Literal`(value: Float): Literal = Literal(value, FloatType)

  implicit def `Double->Literal`(value: Double): Literal = Literal(value, DoubleType)

  implicit def `String->Literal`(value: String): Literal = Literal(value, StringType)

  implicit class OfDataType(value: Any) {
    def of(dataType: DataType): Literal = Literal(value, dataType)
  }

  implicit def `Symbol->UnresolvedAttribute`(name: Symbol): UnresolvedAttribute =
    UnresolvedAttribute(name)

  implicit def `Name->UnresolvedAttribute`(name: Name): UnresolvedAttribute =
    UnresolvedAttribute(name)

  implicit class StringToUnresolvedAttribute(sc: StringContext) {
    def $(args: Any*): UnresolvedAttribute =
      (new Parser).parseAttribute(sc.s(args: _*))
  }

  private[scraper] implicit class NamedExpressionSet[E <: NamedExpression](set: Set[E]) {
    require(set forall (_.isResolved))

    def intersectByID(other: Set[E]): Set[E] = {
      require(other forall (_.isResolved))
      val otherIDs = other map (_.expressionID)
      set filter (e => otherIDs contains e.expressionID)
    }

    def subsetOfByID(other: Set[E]): Boolean = {
      require(other forall (_.isResolved))
      val otherIDs = other map (_.expressionID)
      set forall (e => otherIDs contains e.expressionID)
    }
  }

  def function(name: Name, args: Expression*): UnresolvedFunction =
    UnresolvedFunction(name, args, distinct = false)

  def distinctFunction(name: Name, args: Expression*): UnresolvedFunction =
    UnresolvedFunction(name, args, distinct = true)

  private[scraper] implicit class TypeConstraintDSL(input: Seq[Expression]) {
    def pass: PassThrough = PassThrough(input)

    def sameTypeAs(dataType: DataType): SameTypeAs = SameTypeAs(dataType, input)

    def sameSubtypeOf(supertype: AbstractDataType): SameSubtypesOf =
      SameSubtypesOf(supertype, input)

    def sameType: SameType = SameType(input)

    def foldable: Foldable = Foldable(input)
  }
}
