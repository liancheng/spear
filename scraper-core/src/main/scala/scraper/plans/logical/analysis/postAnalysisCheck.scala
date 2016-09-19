package scraper.plans.logical.analysis

import scraper._
import scraper.exceptions.{AnalysisException, ResolutionFailureException}
import scraper.expressions._
import scraper.expressions.aggregates.DistinctAggregateFunction
import scraper.plans.logical._
import scraper.utils._

class RejectUnresolvedExpressions(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = {
    tree.transformExpressionsDown {
      // Tries to collect a "minimum" unresolved expression.
      case Unresolved(e) if e.children forall (_.isResolved) =>
        throw new ResolutionFailureException(
          s"""Failed to resolve expression ${e.debugString} in the analyzed logical plan:
             |
             |${tree.prettyTree}
             |""".stripMargin
        )
    }
  }
}

class RejectUnresolvedPlans(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = {
    tree.transformDown {
      // Tries to collect a "minimum" unresolved logical plan node.
      case Unresolved(plan) if plan.children.forall(_.isResolved) =>
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
}

class RejectGeneratedAttributes(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = {
    val generated = tree.output.collect { case e: GeneratedNamedExpression => e }

    if (generated.nonEmpty) {
      val generatedList = generated mkString ("[", ", ", "]")
      val suggestion =
        """You probably hit an internal bug since generated attributes are only used internally
          |by the analyzer and should never appear in a fully analyzed logical plan.
          |""".oneLine

      throw new ResolutionFailureException(
        s"""Generated attributes $generatedList found in the analyzed logical plan:
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

class RejectDistinctAggregateFunctions(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = {
    val distinctAggs = tree collectFromAllExpressions {
      case agg: DistinctAggregateFunction => agg
    }

    if (distinctAggs.nonEmpty) {
      val distinctAggList = distinctAggs mkString ("[", ", ", "]")
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

class RejectOrphanAttributeReferences(val catalog: Catalog) extends AnalysisRule {
  override def apply(tree: LogicalPlan): LogicalPlan = tree.transformUp {
    case plan: LeafLogicalPlan =>
      plan

    case plan =>
      val inputSet = plan.children.flatMap(_.outputSet) ++ plan.derivedOutput
      val orphans = plan.references filterNot inputSet.contains

      if (orphans.nonEmpty) {
        val message =
          s"""Orphan attribute references ${orphans mkString ("[", ", ", "]")} found in the
             |following logical plan operator. They are neither output of child operators nor
             |derived by the problematic operator itself.
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
