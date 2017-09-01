package spear.plans.logical.analysis

import spear._
import spear.exceptions.{AnalysisException, ResolutionFailureException}
import spear.expressions._
import spear.expressions.aggregates.DistinctAggregateFunction
import spear.plans.logical._
import spear.plans.logical.patterns.Unresolved
import spear.utils._

class RejectUnresolvedExpression(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = tree.transformExpressionsDown {
    // Tries to collect a "minimum" unresolved expression.
    case Unresolved(e) if e.children forall { _.isResolved } =>
      throw new ResolutionFailureException(
        s"""Failed to resolve expression ${e.sqlLike} in the analyzed logical plan:
           |
           |${tree.prettyTree}
           |""".stripMargin
      )
  }
}

class RejectUnresolvedPlan(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = tree.transformDown {
    // Tries to collect a "minimum" unresolved logical plan node.
    case Unresolved(plan) if plan.children forall { _.isResolved } =>
      throw new ResolutionFailureException(
        s"""Failed to resolve the following logical plan operator
           |
           |${plan.prettyTree}
           |
           |in the analyzed plan:
           |
           |${tree.prettyTree}
           |""".stripMargin
      )
  }
}

class RejectTopLevelInternalAttribute(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = {
    val internal = tree.output.collect { case e: NamedExpression if e.namespace.nonEmpty => e }

    if (internal.nonEmpty) {
      val internalList = internal map { _.sqlLike } mkString ("[", ", ", "]")
      val suggestion =
        """You probably hit an internal bug since internal attributes are only used internally by
          |the analyzer and should never appear in a fully analyzed logical plan.
          |""".oneLine

      throw new ResolutionFailureException(
        s"""Internal attributes $internalList found in the analyzed logical plan:
           |
           |${tree.prettyTree}
           |
           |$suggestion
           |""".stripMargin
      )
    }

    tree
  }
}

class RejectDistinctAggregateFunction(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = {
    val distinctAggs = tree.collectDown {
      case node =>
        node.expressions flatMap {
          _ collectDown {
            case agg: DistinctAggregateFunction => agg
          }
        }
    }.flatten

    if (distinctAggs.nonEmpty) {
      val distinctAggList = distinctAggs map { _.sqlLike } mkString ("[", ", ", "]")
      val suggestion =
        """You probably hit an internal bug since all distinct aggregate functions should have
          |been resolved into normal aggregate functions by the analyzer.
          |""".oneLine

      throw new ResolutionFailureException(
        s"""Distinct aggregate functions $distinctAggList found in the analyzed logical plan:
           |
           |${tree.prettyTree}
           |
           |$suggestion
           |""".stripMargin
      )
    }

    tree
  }
}

class RejectOrphanAttributeReference(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case plan: LeafLogicalPlan =>
      plan

    case plan =>
      val inputSet = plan.children.flatMap { _.outputSet } ++ plan.derivedInput
      val orphans = plan.references filterNot inputSet.contains

      if (orphans.nonEmpty) {
        val message =
          s"""Orphan attribute references ${orphans map { _.sqlLike } mkString ("[", ", ", "]")}
             |found in the following logical plan operator. They are neither output of child
             |operators nor derived by the problematic operator itself.
             |""".oneLine

        throw new AnalysisException(
          s"""$message
             |
             |${plan.prettyTree}
             |""".stripMargin
        )
      }

      plan
  }
}
