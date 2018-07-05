package spear.parsers

import fastparse.all.P

import spear.plans.logical.LogicalPlan

// SQL06 section 22.1
object DirectlyExecutableStatementParser extends LoggingParser {
  import OrderByClauseParser._
  import QueryExpressionParser._
  import WhitespaceApi.parserApi

  private val cursorSpecification: P[LogicalPlan] = queryExpression ~ orderByClause.? map {
    case (plan, orderBy) => orderBy.orIdentity apply plan
  } opaque "cursor-specification"

  private val directSelectStatementMultipleRows: P[LogicalPlan] =
    cursorSpecification opaque "direct-select-statement-multiple-rows"

  private val directSQLDataStatement: P[LogicalPlan] =
    directSelectStatementMultipleRows opaque "direct-sql-data-statement"

  val directlyExecutableStatement: P[LogicalPlan] =
    directSQLDataStatement opaque "directly-executable-statement"
}
