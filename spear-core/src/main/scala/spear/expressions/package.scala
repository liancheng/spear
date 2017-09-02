package spear

import scala.language.implicitConversions

import spear.expressions.dsl.ExpressionDSL
import spear.expressions.typecheck._
import spear.parsers._
import spear.types._

package object expressions extends expressions.dsl.LowPriorityImplicits {
  val * : Star = Star(None)

  implicit def `Expression->ExpressionDSL`(expression: Expression): ExpressionDSL =
    new ExpressionDSL {
      override val self: Expression = expression
    }

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

  implicit class ParsedUnresolvedAttribute(sc: StringContext) {
    import ColumnReferenceParser._
    import QuerySpecificationParser._
    import fastparse.all._

    private val parser: P[NamedExpression] = (
      ("*" ==> *)
      | qualifiedAsterisk
      | columnReference
    )

    def $(args: Any*): NamedExpression = (parser parse sc.s(args: _*)).get.value
  }

  private[spear] implicit class NamedExpressionSet[E <: NamedExpression](set: Set[E]) {
    require(set forall { _.isResolved })

    def intersectByID(other: Set[E]): Set[E] = {
      require(other forall { _.isResolved })
      val otherIDs = other map { _.expressionID }
      set filter { e => otherIDs contains e.expressionID }
    }

    def subsetOfByID(other: Set[E]): Boolean = {
      require(other forall { _.isResolved })
      val otherIDs = other map { _.expressionID }
      set forall { e => otherIDs contains e.expressionID }
    }
  }

  def function(name: Name, args: Expression*): UnresolvedFunction =
    UnresolvedFunction(name, args, isDistinct = false)

  def distinctFunction(name: Name, args: Expression*): UnresolvedFunction =
    UnresolvedFunction(name, args, isDistinct = true)

  implicit class UnresolvedFunctionDSL(name: Symbol) {
    def apply(args: Expression*): UnresolvedFunction = function(name, args: _*)
  }

  implicit class TypeConstraintDSL(input: Seq[Expression]) {
    def anyType: TypeConstraint = StrictlyTyped(input)

    def sameTypeAs(dataType: DataType): TypeConstraint = SameTypeAs(input, dataType)

    def sameSubtypeOf(supertype: AbstractDataType): TypeConstraint = SameSubtypeOf(input, supertype)

    def sameType: TypeConstraint = SameType(input)

    def foldable: TypeConstraint = Foldable(input)
  }

  implicit class SingleExpressionTypeConstraintDSL(input: Expression) {
    def anyType: TypeConstraint = StrictlyTyped(Seq(input))

    def sameTypeAs(dataType: DataType): TypeConstraint = Seq(input) sameTypeAs dataType

    def subtypeOf(supertype: AbstractDataType): TypeConstraint = Seq(input) sameSubtypeOf supertype

    def oneOf(first: AbstractDataType, rest: AbstractDataType*): TypeConstraint =
      Seq(input) sameSubtypeOf OneOf(first +: rest)

    def foldable: TypeConstraint = Foldable(Seq(input))

    def literalCastableTo(dataType: DataType): TypeConstraint = new TypeConstraint {
      override def enforced: Seq[Expression] = Seq((input cast dataType).evaluated of dataType)
    }
  }

  implicit class Invoker(expression: Expression) {
    def invoke(methodName: String, returnType: DataType): InvokeBuilder =
      new InvokeBuilder(methodName, returnType)

    class InvokeBuilder(methodName: String, returnType: DataType) {
      def withArgs(args: Expression*): Invoke = Invoke(expression, methodName, returnType, args)

      def noArgs: Invoke = Invoke(expression, methodName, returnType, Nil)
    }
  }

  implicit class StaticInvoker(targetClass: Class[_]) {
    def invoke(methodName: String, returnType: DataType): StaticInvokeBuilder =
      new StaticInvokeBuilder(methodName, returnType)

    class StaticInvokeBuilder(methodName: String, returnType: DataType) {
      def withArgs(args: Expression*): StaticInvoke =
        StaticInvoke(targetClass.getName, methodName, returnType, args)

      def noArgs: StaticInvoke =
        StaticInvoke(targetClass.getName, methodName, returnType, Nil)
    }
  }
}
