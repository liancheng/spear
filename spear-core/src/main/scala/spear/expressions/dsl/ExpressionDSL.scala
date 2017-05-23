package spear.expressions.dsl

import scala.language.implicitConversions

import spear.Name
import spear.expressions._
import spear.expressions.NamedExpression.newExpressionID
import spear.expressions.windows.{Window, WindowFunction, WindowSpec}
import spear.types.DataType

trait ExpressionDSL {
  val self: Expression

  def +(that: Expression): Plus = Plus(self, that)

  def -(that: Expression): Minus = Minus(self, that)

  def *(that: Expression): Multiply = Multiply(self, that)

  def /(that: Expression): Divide = Divide(self, that)

  def %(that: Expression): Remainder = Remainder(self, that)

  def ^(that: Expression): Power = Power(self, that)

  def unary_- : Negate = Negate(self)

  def >(that: Expression): Gt = Gt(self, that)

  def <(that: Expression): Lt = Lt(self, that)

  def >=(that: Expression): GtEq = GtEq(self, that)

  def <=(that: Expression): LtEq = LtEq(self, that)

  def ===(that: Expression): Eq = Eq(self, that)

  def =/=(that: Expression): NotEq = NotEq(self, that)

  def <=>(that: Expression): NullSafeEq = NullSafeEq(self, that)

  def &&(that: Expression): And = And(self, that)

  def ||(that: Expression): Or = Or(self, that)

  def unary_! : Not = Not(self)

  def isNaN: IsNaN = IsNaN(self)

  def as(alias: Name): Alias = Alias(self, alias, newExpressionID())

  def cast(dataType: DataType): Cast = Cast(self, dataType)

  def isNull: IsNull = IsNull(self)

  def isNotNull: IsNotNull = IsNotNull(self)

  def asc: SortOrder = SortOrder(self, Ascending, isNullLarger = true)

  def desc: SortOrder = SortOrder(self, Descending, isNullLarger = true)

  def in(list: Seq[Expression]): In = In(self, list)

  def in(first: Expression, rest: Expression*): In = In(self, first +: rest)

  def rlike(pattern: Expression): RLike = RLike(self, pattern)

  def over(window: WindowSpec): WindowFunction = WindowFunction(self, window)

  def over(windowName: Name): WindowFunction = WindowFunction(self, Window(windowName))

  def over(): WindowFunction = WindowFunction(self, Window.Default)
}

trait LowPriorityImplicits {
  implicit def `Boolean->ExpressionDSL`(value: Boolean): ExpressionDSL = new ExpressionDSL {
    override val self: Expression = `Boolean->Literal`(value)
  }

  implicit def `Byte->ExpressionDSL`(value: Byte): ExpressionDSL = new ExpressionDSL {
    override val self: Expression = `Byte->Literal`(value)
  }

  implicit def `Short->ExpressionDSL`(value: Short): ExpressionDSL = new ExpressionDSL {
    override val self: Expression = `Short->Literal`(value)
  }

  implicit def `Int->ExpressionDSL`(value: Int): ExpressionDSL = new ExpressionDSL {
    override val self: Expression = `Int->Literal`(value)
  }

  implicit def `Long->ExpressionDSL`(value: Long): ExpressionDSL = new ExpressionDSL {
    override val self: Expression = `Long->Literal`(value)
  }

  implicit def `Float->ExpressionDSL`(value: Float): ExpressionDSL = new ExpressionDSL {
    override val self: Expression = `Float->Literal`(value)
  }

  implicit def `Double->ExpressionDSL`(value: Double): ExpressionDSL = new ExpressionDSL {
    override val self: Expression = `Double->Literal`(value)
  }

  implicit def `String->ExpressionDSL`(value: String): ExpressionDSL = new ExpressionDSL {
    override val self: Expression = `String->Literal`(value)
  }

  implicit def `Symbol->ExpressionDSL`(name: Symbol): ExpressionDSL = new ExpressionDSL {
    override val self: Expression = `Symbol->UnresolvedAttribute`(name)
  }

  implicit def `Name->ExpressionDSL`(name: Name): ExpressionDSL = new ExpressionDSL {
    override val self: Expression = `Name->UnresolvedAttribute`(name)
  }
}
