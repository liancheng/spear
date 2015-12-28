package scraper.expressions.dsl

import scraper.expressions._

trait ComparisonDSL { this: Expression =>
  def >(that: Expression): Gt = Gt(this, that)

  def <(that: Expression): Lt = Lt(this, that)

  def >=(that: Expression): GtEq = GtEq(this, that)

  def <=(that: Expression): LtEq = LtEq(this, that)

  def =:=(that: Expression): Eq = Eq(this, that)

  def =/=(that: Expression): NotEq = NotEq(this, that)
}
