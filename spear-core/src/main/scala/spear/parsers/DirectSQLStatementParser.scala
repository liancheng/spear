package spear.parsers

import fastparse.all._

import spear.plans.logical.LogicalPlan

object DirectSQLStatementParser extends LoggingParser {
  import DirectlyExecutableStatementParser._

  val directSQLStatement: P[LogicalPlan] = directlyExecutableStatement opaque "direct-SQL-statement"
}

object DirectlyExecutableStatementParser extends LoggingParser {
  import OrderByClauseParser._
  import QueryExpressionParser._
  import WhitespaceApi._

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
