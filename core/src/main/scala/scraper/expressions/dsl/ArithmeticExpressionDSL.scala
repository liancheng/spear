package scraper.expressions.dsl

import scraper.expressions._

trait ArithmeticExpressionDSL { this: Expression =>
  def +(that: Expression): Plus = Plus(this, that)

  def -(that: Expression): Minus = Minus(this, that)

  def *(that: Expression): Multiply = Multiply(this, that)

  def /(that: Expression): Divide = Divide(this, that)

  def %(that: Expression): Remainder = Remainder(this, that)

  def unary_- : Negate = Negate(this)

  def isNaN: IsNaN = IsNaN(this)
}
