package scraper.expressions

import scala.language.implicitConversions

import scraper.config.Settings
import scraper.expressions.typecheck._
import scraper.parser.DeprecatedParser
import scraper.types._

package object dsl {
  implicit def `Boolean->Literal`(value: Boolean): Literal = Literal(value, BooleanType)

  implicit def `Byte->Literal`(value: Byte): Literal = Literal(value, ByteType)

  implicit def `Short->Literal`(value: Short): Literal = Literal(value, ShortType)

  implicit def `Int->Literal`(value: Int): Literal = Literal(value, IntType)

  implicit def `Long->Literal`(value: Long): Literal = Literal(value, LongType)

  implicit def `Float->Literal`(value: Float): Literal = Literal(value, FloatType)

  implicit def `Double->Literal`(value: Double): Literal = Literal(value, DoubleType)

  implicit def `String->Literal`(value: String): Literal = Literal(value, StringType)

  implicit def `Symbol->UnresolvedAttribute`(name: Symbol): UnresolvedAttribute =
    UnresolvedAttribute(name.name)

  implicit class StringToUnresolvedAttribute(sc: StringContext) {
    def $(args: Any*): UnresolvedAttribute =
      new DeprecatedParser(Settings.empty).parseAttribute(sc.s(args: _*))
  }

  private[scraper] trait BinaryOperatorPattern[T <: BinaryExpression] {
    def unapply(op: T): Option[(Expression, Expression)] = Some((op.left, op.right))
  }

  private[scraper] object ! {
    def unapply(not: Not): Option[Expression] = Some(not.child)
  }

  private[scraper] object || extends BinaryOperatorPattern[Or]

  private[scraper] object && extends BinaryOperatorPattern[And]

  private[scraper] object =:= extends BinaryOperatorPattern[Eq]

  private[scraper] object =/= extends BinaryOperatorPattern[NotEq]

  private[scraper] object > extends BinaryOperatorPattern[Gt]

  private[scraper] object < extends BinaryOperatorPattern[Lt]

  private[scraper] object >= extends BinaryOperatorPattern[GtEq]

  private[scraper] object <= extends BinaryOperatorPattern[LtEq]

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

  def function(name: String, args: Expression*): UnresolvedFunction = UnresolvedFunction(name, args)

  def function(name: Symbol, args: Expression*): UnresolvedFunction = function(name.name, args: _*)

  private[scraper] implicit class TypeConstraintDSL(input: Seq[Expression]) {
    def sameTypeAs(dataType: DataType): SameTypeAs = SameTypeAs(dataType, input)

    def sameSubtypeOf(supertype: AbstractDataType): SameSubtypesOf = SameSubtypesOf(supertype, input)

    def sameType: SameType = SameType(input)
  }
}
