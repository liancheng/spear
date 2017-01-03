package scraper.plans

import scraper.Name
import scraper.expressions.Expression

package object logical {
  def table(name: Name): UnresolvedRelation = UnresolvedRelation(name)

  def values(expressions: Seq[Expression]): Project = SingleRowRelation select expressions

  def values(first: Expression, rest: Expression*): Project = values(first +: rest)

  def let(name: Name, plan: LogicalPlan)(body: => LogicalPlan): With = {
    With(body, name, plan, None)
  }

  def let(name: Name, plan: LogicalPlan, aliases: Seq[Name])(body: => LogicalPlan): With = {
    With(body, name, plan, Some(aliases))
  }
}
