package scraper.expressions.dsl

import scraper.expressions._

trait LogicalOperatorDSL { this: Expression =>
  def &&(that: Expression): And = And(this, that)

  def ||(that: Expression): Or = Or(this, that)

  def unary_! : Not = Not(this)
}
