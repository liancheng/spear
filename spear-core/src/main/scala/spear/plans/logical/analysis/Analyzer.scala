package spear.plans.logical.analysis

import spear._
import spear.Name.caseInsensitive
import spear.exceptions.AnalysisException
import spear.expressions._
import spear.expressions.NamedExpression.newExpressionID
import spear.plans.logical._
import spear.plans.logical.analysis.AggregationAnalysis.hasAggregateFunction
import spear.plans.logical.patterns.{Resolved, Unresolved}
import spear.trees._

class Analyzer(catalog: Catalog) extends Transformer(Analyzer.defaultPhases(catalog)) {
  override def apply(tree: LogicalPlan): LogicalPlan = {
    logDebug(
      s"""Analyzing logical query plan:
         |
         |${tree.prettyTree}
         |""".stripMargin
    )

    super.apply(tree)
  }
}

object Analyzer {
  def defaultPhases(catalog: Catalog): Seq[RuleGroup[LogicalPlan]] = Seq(
    RuleGroup("Pre-processing", FixedPoint, Seq(
      new RewriteCTEsAsSubquery(catalog),
      new InlineWindowDefinitions(catalog)
    )),

    RuleGroup("Pre-processing check", Once, Seq(
      new RejectUndefinedWindowSpecRef(catalog)
    )),

    RuleGroup("Resolution", FixedPoint, Seq(
      new ResolveRelation(catalog),
      new RewriteRenameToProject(catalog),
      new RewriteUnresolvedSort(catalog),
      new DeduplicateReferences(catalog),

      // Rules that help resolving expressions
      new ExpandStar(catalog),
      new ResolveReference(catalog),
      new ResolveFunction(catalog),
      new ResolveAlias(catalog),

      // Rules that help resolving window functions
      new ExtractWindowFunctions(catalog),

      // Rules that help resolving aggregations
      new RewriteDistinctAggregateFunction(catalog),
      new RewriteDistinctToAggregate(catalog),
      new RewriteProjectToGlobalAggregate(catalog),
      new UnifyFilteredSortedAggregate(catalog),
      new RewriteUnresolvedAggregate(catalog)
    )),

    RuleGroup("Type check", Once, Seq(
      new EnforceTypeConstraint(catalog)
    )),

    RuleGroup("Post-analysis check", Once, Seq(
      new RejectUnresolvedExpression(catalog),
      new RejectUnresolvedPlan(catalog),
      new RejectTopLevelInternalAttribute(catalog),
      new RejectDistinctAggregateFunction(catalog),
      new RejectOrphanAttributeReference(catalog)
    ))
  )
}

trait AnalysisRule extends Rule[LogicalPlan] {
  val catalog: Catalog
}

/**
 * This rule rewrites CTE relation definitions as sub-queries. E.g., it transforms
 * {{{
 *   WITH
 *     c0 AS (SELECT * FROM t0),
 *     c1 AS (SELECT * FROM t1)
 *   SELECT * FROM c0
 *   UNION ALL
 *   SELECT * FROM c1
 * }}}
 * into
 * {{{
 *   SELECT * FROM (SELECT * FROM t0) c0
 *   UNION ALL
 *   SELECT * FROM (SELECT * FROM t1) c1
 * }}}
 */
class RewriteCTEsAsSubquery(val catalog: Catalog) extends AnalysisRule {
  // Uses `transformUp` to inline all CTE relations from bottom up since inner CTE relations may
  // shadow outer CTE relations with the same names.
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case With(child, name, query, maybeAliases) =>
      child transformUp {
        case UnresolvedRelation(`name`) =>
          (maybeAliases fold query) { query rename _ } subquery name
      }
  }
}

/**
 * This rule resolves unresolved relations by looking up the table name from the `catalog`.
 */
class ResolveRelation(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case UnresolvedRelation(name) => catalog lookupRelation name
  }
}

/**
 * This rule rewrites [[Rename]] operators into [[Project projections]] to help resolve CTE queries.
 */
class RewriteRenameToProject(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case Resolved(child) Rename aliases Subquery name if child.output.length >= aliases.length =>
      val aliasCount = aliases.length
      val aliased = (child.output take aliasCount, aliases).zipped map { _ as _ }
      child select (aliased ++ (child.output drop aliasCount)) subquery name

    case Resolved(child) Rename aliases Subquery name =>
      val expected = child.output.length
      val actual = aliases.length
      throw new AnalysisException(
        s"WITH query $name has $expected columns available but $actual columns specified"
      )
  }
}

/**
 * This rule resolves ambiguous duplicated attributes/aliases introduced by binary logical query
 * plan operators like [[Join]] and [[SetOperator set operators]]. For example:
 * {{{
 *   // Self-join, equivalent to "SELECT * FROM t INNER JOIN t":
 *   val df = context table "t"
 *   val joined = df join df
 *
 *   // Self-union, equivalent to "SELECT 1 AS a UNION ALL SELECT 1 AS a":
 *   val df = context single (1 as 'a)
 *   val union = df union df
 * }}}
 */
class DeduplicateReferences(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case plan if plan.children.length < 2 =>
      plan

    case plan if plan.children exists { !_.isResolved } =>
      plan

    case plan: BinaryLogicalPlan if !plan.isDeduplicated =>
      plan.withChildren(plan.left :: deduplicateRight(plan.left, plan.right) :: Nil)
  }

  private def deduplicateRight(left: LogicalPlan, right: LogicalPlan): LogicalPlan = {
    val conflictingAttributes = left.outputSet intersectByID right.outputSet

    def hasDuplicates(attributes: Set[Attribute]): Boolean =
      (attributes intersectByID conflictingAttributes).nonEmpty

    def rewriteExpressionIDs(oldPlan: LogicalPlan, newPlan: LogicalPlan): LogicalPlan = {
      val rewrite = {
        val oldIDs = oldPlan.output.map { _.expressionID }
        val newIDs = newPlan.output.map { _.expressionID }
        (oldIDs zip newIDs).toMap
      }

      right transformDown {
        case plan if plan == oldPlan => newPlan
      } transformDown {
        case node =>
          node transformExpressionsDown {
            case a: AttributeRef => a withID rewrite.getOrElse(a.expressionID, a.expressionID)
          }
      }
    }

    val maybeDuplicated = right collectFirstDown {
      // Handles relations that introduce ambiguous attributes
      case plan: MultiInstanceRelation if hasDuplicates(plan.outputSet) =>
        plan -> plan.newInstance()

      // Handles projections that introduce ambiguous aliases
      case plan @ Project(_, projectList) if hasDuplicates(collectAliases(projectList)) =>
        val newProjectList = projectList map {
          case a: Alias => a withID newExpressionID()
          case e        => e
        }

        plan -> plan.copy(projectList = newProjectList)
    }

    maybeDuplicated map {
      case (oldPlan, newPlan) =>
        rewriteExpressionIDs(oldPlan, newPlan)
    } getOrElse right
  }

  private def collectAliases(projectList: Seq[NamedExpression]): Set[Attribute] =
    projectList.collect { case a: Alias => a.attr }.toSet
}

/**
 * This rule allows a SQL `ORDER BY` clause to reference columns from both the `FROM` clause and the
 * `SELECT` clause. E.g., assuming table `t` consists of a single `INT` column `a`, the logical
 * query plan parsed from the following SQL query
 * {{{
 *   SELECT a + 1 AS x FROM t ORDER BY a
 * }}}
 * may look like
 * {{{
 *   UnresolvedSort order=[a]
 *   +- Project projectList=[a + 1 AS x]
 *      +- Relation name=t, output=[a]
 * }}}
 * This plan tree is not yet valid because the attribute `a` referenced by `Sort` is not an output
 * attribute of `Project`. This rule tries to resolve this issue by rewriting the above plan into
 * {{{
 *   Project projectList=[x] => [x]
 *   +- Sort order=[a] => [x, a]
 *      +- Project projectList=[a + 1 AS x, a] => [x, a]
 *         +- Relation name=t => [a]
 * }}}
 * Another more convoluted example is about global aggregate:
 * {{{
 *   SELECT 1 AS x FROM t ORDER BY count(a)
 * }}}
 * The aggregate function call `count(a)` in the `ORDER BY` clause makes this query into a global
 * aggregation, but we've no idea about that during parsing phase since the parser doesn't know
 * whether `count(a)` is an aggregate function or not. Therefore, it's parsed into a simple
 * projection:
 * {{{
 *   UnresolvedSort order=[count(a)] => ???
 *   +- Project projectList=[1 AS x] => [x]
 *      +- Relation name=t => [a]
 * }}}
 * Then this rule rewrites it into:
 * {{{
 *   Project projectList=[x] => [x]
 *   +- Sort order=[order0] => [x, order0]
 *      +- Project projectList=[1 AS x, count(a) AS order0] => [x, order0]
 *         +- Relation name=t => [a]
 * }}}
 * Now the `count(a)` function call can be successfully resolved and later the projection can be
 * rewritten into a global projection by the [[RewriteProjectToGlobalAggregate]] analysis rule:
 * {{{
 *   Project projectList=[x] => ???
 *   +- Sort order=[order0] => ???
 *      +- UnresolvedAggregate projectList=[1 AS x, count(a) AS order0] => ???
 *         +- Relation name=t => [a]
 * }}}
 */
class RewriteUnresolvedSort(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformDown {
    case plan @ Unresolved(_ Project _) UnresolvedSort _ =>
      plan

    case plan @ child Project projectList UnresolvedSort order =>
      val output = projectList map { _.attr }

      val maybeResolvedOrder = order
        .map { _ tryResolveUsing output }
        .map { case e: SortOrder => e }

      // Finds out all unevaluable `SortOrder`s containing either unresolved expressions or
      // aggregate functions (no matter resolved or not, since aggregate functions cannot be
      // evaluated as part of a `Sort` operator).
      val unevaluableOrder = maybeResolvedOrder.filter { order =>
        !order.isResolved || hasAggregateFunction(order)
      }

      if (unevaluableOrder.isEmpty) {
        child select projectList sort maybeResolvedOrder
      } else {
        // Pushes down unevaluable `SortOrder`s by adding an intermediate projection.
        val pushDown = unevaluableOrder
          .map { _.child }
          .map { _ unaliasUsing projectList }
          .zipWithIndex
          .map { case (e, index) => SortOrderAlias(e, caseInsensitive(s"order$index")) }

        val rewrite = pushDown.map { e => e.child -> (e.name: Expression) }.toMap

        child
          .select(projectList ++ pushDown)
          .sort(maybeResolvedOrder map { _ transformUp rewrite })
          .select(output)
      }

    case child UnresolvedSort order =>
      child sort order
  }
}

/**
 * This rule tries to transform all resolved logical plans operators (and expressions within them)
 * into strictly-typed form.
 *
 * @throws spear.exceptions.AnalysisException If some resolved logical query plan operator
 *         doesn't type check.
 */
class EnforceTypeConstraint(val catalog: Catalog) extends AnalysisRule {
  override def transform(tree: LogicalPlan): LogicalPlan = tree transformUp {
    case Resolved(plan) => plan.strictlyTyped
  }
}
