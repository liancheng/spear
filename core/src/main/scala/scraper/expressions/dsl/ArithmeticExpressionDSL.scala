package scraper.expressions.dsl

import scraper.expressions._

trait ArithmeticExpressionDSL { this: Expression =>
  def +(that: Expression): Add = Add(this, that)

  def -(that: Expression): Minus = Minus(this, that)

  def *(that: Expression): Multiply = Multiply(this, that)

  def /(that: Expression): Divide = Divide(this, that)

  def unary_- : Negate = Negate(this)

  def unary_+ : Positive = Positive(this)

  def isNaN: IsNaN = IsNaN(this)
}
