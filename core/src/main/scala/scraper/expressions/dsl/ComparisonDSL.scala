package scraper.expressions.dsl

import scraper.expressions._

trait ComparisonDSL { this: Expression =>
  def >(that: Expression): Gt = Gt(this, that)

  def gt(that: Expression): Gt = this > that

  def <(that: Expression): Lt = Lt(this, that)

  def lt(that: Expression): Lt = this < that

  def >=(that: Expression): GtEq = GtEq(this, that)

  def gteq(that: Expression): GtEq = this >= that

  def <=(that: Expression): LtEq = LtEq(this, that)

  def lteq(that: Expression): LtEq = this <= that

  def =:=(that: Expression): Eq = Eq(this, that)

  def equalsTo(that: Expression): Eq = this =:= that

  def =/=(that: Expression): NotEq = NotEq(this, that)

  def notEqual(that: Expression): NotEq = this =/= that

  def <=>(that: Expression): NullSafeEq = NullSafeEq(this, that)
}
