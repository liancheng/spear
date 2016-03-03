package scraper.expressions.dsl

import scraper.expressions._

trait ArithmeticExpressionDSL { this: Expression =>
  def +(that: Expression): Plus = Plus(this, that)

  def plus(that: Expression): Plus = Plus(this, that)

  def -(that: Expression): Minus = Minus(this, that)

  def minus(that: Expression): Minus = this - that

  def *(that: Expression): Multiply = Multiply(this, that)

  def mul(that: Expression): Multiply = this * that

  def /(that: Expression): Divide = Divide(this, that)

  def div(that: Expression): Divide = this / that

  def unary_- : Negate = Negate(this)

  def neg: Negate = -this

  def isNaN: IsNaN = IsNaN(this)
}
