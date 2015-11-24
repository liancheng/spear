package scraper.expressions.dsl

import scraper.expressions.{And, Not, Or, Predicate}

trait LogicalOperatorDSL { this: Predicate =>
  def &&(that: Predicate): And = And(this, that)

  def and(that: Predicate): And = this && that

  def ||(that: Predicate): Or = Or(this, that)

  def or(that: Predicate): Or = this || that

  def unary_! : Not = Not(this)
}
