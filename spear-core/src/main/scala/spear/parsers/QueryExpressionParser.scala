package spear.parsers

import fastparse.all._

import spear.Name
import spear.annotations.ExtendedSQLSyntax
import spear.exceptions.AnalysisException
import spear.expressions.{*, Expression, NamedExpression, SortOrder}
import spear.expressions.windows._
import spear.plans.logical._

// SQL06 section 7.6
object TableReferenceParser extends LoggingParser {
  import IdentifierParser._
  import JoinedTableParser._
  import KeywordParser._
  import NameParser._
  import SubqueryParser._
  import WhitespaceApi._

  private val correlationClause: P[LogicalPlan => LogicalPlan] = (
    AS.? ~ identifier
    map { name => (_: LogicalPlan) subquery name }
    opaque "correlation-clause"
  )

  private val tableOrQueryName: P[LogicalPlan] =
    tableName map { _.table } map table opaque "table-or-query-name"

  private val derivedTable: P[LogicalPlan] =
    tableSubquery opaque "derived-table"

  private val tablePrimary: P[LogicalPlan] = (
    tableOrQueryName ~ correlationClause.?.map { _.orIdentity }
    | derivedTable ~ correlationClause
    map { case (name, f) => f(name) }
    opaque "table-primary"
  )

  val tableFactor: P[LogicalPlan] = tablePrimary opaque "table-factor"

  val columnNameList: P[Seq[Name]] = columnName rep (min = 1, sep = ",") opaque "column-name-list"

  private val tablePrimaryOrJoinedTable: P[LogicalPlan] =
    joinedTable | tablePrimary opaque "table-primary-or-joined-table"

  val tableReference: P[LogicalPlan] = tablePrimaryOrJoinedTable opaque "table-reference"
}

// SQL06 section 7.7
object JoinedTableParser extends LoggingParser {
  import KeywordParser._
  import SearchConditionParser._
  import TableReferenceParser._
  import WhitespaceApi._

  private val outerJoinType: P[JoinType] = (
    (LEFT attach LeftOuter)
    | (RIGHT attach RightOuter)
    | (FULL attach FullOuter)
  ) ~ OUTER.? opaque "outer-join-type"

  private val joinType: P[JoinType] =
    (INNER attach Inner) | outerJoinType opaque "joinType"

  private val joinCondition: P[Expression] =
    ON ~ searchCondition opaque "join-condition"

  private val joinSpecification: P[Expression] =
    joinCondition opaque "join-specification"

  private val qualifiedJoin: P[LogicalPlan] = (
    (P(tableFactor) | P(tableReference))
    ~ joinType.? ~ JOIN
    ~ P(tableReference)
    ~ joinSpecification.? map {
      case (lhs, maybeType, rhs, maybeCondition) =>
        lhs join (rhs, maybeType getOrElse Inner) on maybeCondition.toSeq
    } opaque "qualified-join"
  )

  private val crossJoin: P[LogicalPlan] =
    (P(tableFactor) | P(tableReference)) ~ CROSS ~ JOIN ~ tableFactor map {
      case (lhs, rhs) => lhs join (rhs, Inner)
    } opaque "cross-join"

  val joinedTable: P[LogicalPlan] = crossJoin | qualifiedJoin opaque "joined-table"
}

// SQL06 section 7.9
object GroupByClauseParser extends LoggingParser {
  import ColumnReferenceParser._
  import KeywordParser._
  import ValueExpressionParser._
  import WhitespaceApi._

  @ExtendedSQLSyntax
  private val groupingColumnReference: P[Expression] =
    valueExpression | columnReference opaque "grouping-column-reference"

  private val groupingColumnReferenceList: P[Seq[Expression]] =
    groupingColumnReference rep (min = 1, sep = ",") opaque "grouping-column-reference-list"

  private val ordinaryGroupingSet: P[Seq[Expression]] = (
    groupingColumnReference.map { _ :: Nil }
    | "(" ~ groupingColumnReferenceList ~ ")"
    opaque "ordinary-grouping-set"
  )

  private val groupingElement: P[Seq[Expression]] = ordinaryGroupingSet opaque "grouping-element"

  private val groupingElementList: P[Seq[Expression]] = (
    groupingElement
    rep (min = 1, sep = ",")
    map { _.flatten }
    opaque "grouping-element-list"
  )

  val groupByClause: P[Seq[Expression]] =
    GROUP ~ BY ~ groupingElementList opaque "group-by-clause"
}

object WindowClauseParser extends LoggingParser {
  import ColumnReferenceParser._
  import KeywordParser._
  import NameParser._
  import SortSpecificationListParser._
  import ValueExpressionParser._
  import ValueSpecificationParser._
  import WhitespaceApi._

  private val existingWindowName: P[Name] = windowName opaque "existing-window-name"

  @ExtendedSQLSyntax
  private val windowPartitionColumnReference: P[Expression] =
    valueExpression | columnReference opaque "window-partition-column-reference"

  private val windowPartitionColumnReferenceList: P[Seq[Expression]] = (
    windowPartitionColumnReference.rep(min = 1, sep = ",")
    opaque "window-partition-column-reference-list"
  )

  private val windowPartitionClause: P[Seq[Expression]] =
    PARTITION ~ BY ~ windowPartitionColumnReferenceList opaque "window-partition-clause"

  private val windowOrderClause: P[Seq[SortOrder]] =
    ORDER ~ BY ~ sortSpecificationList opaque "window-order-clause"

  private val windowFrameUnits: P[WindowFrameType] =
    (ROWS attach RowsFrame) | (RANGE attach RangeFrame) opaque "window-frame-units"

  private val windowFramePreceding: P[FrameBoundary] =
    unsignedValueSpecification ~ PRECEDING map Preceding opaque "window-frame-preceding"

  private val windowFrameStart: P[FrameBoundary] = (
    (UNBOUNDED ~ PRECEDING attach UnboundedPreceding)
    | windowFramePreceding
    | (CURRENT ~ ROW attach CurrentRow)
    opaque "window-frame-start"
  )

  private val windowFrameFollowing: P[FrameBoundary] =
    unsignedValueSpecification ~ FOLLOWING map Following opaque "window-frame-following"

  private val windowFrameBound: P[FrameBoundary] = (
    windowFrameStart
    | (UNBOUNDED ~ FOLLOWING attach UnboundedFollowing)
    | windowFrameFollowing
    opaque "window-frame-bound"
  )

  private val windowFrameBetween: P[(FrameBoundary, FrameBoundary)] =
    BETWEEN ~ windowFrameBound ~ AND ~ windowFrameBound opaque "window-frame-between"

  private val windowFrameExtent: P[(FrameBoundary, FrameBoundary)] =
    windowFrameStart.map { _ -> CurrentRow } | windowFrameBetween opaque "window-frame-extent"

  private val windowFrameClause: P[WindowFrame] =
    windowFrameUnits ~ windowFrameExtent map {
      case (frameType, (start, end)) => frameType.extent(start, end)
    } opaque "window-frame-clause"

  private val basicWindowSpecification: P[WindowSpec] = (
    windowPartitionClause.?.map { _ getOrElse Nil }
    ~ windowOrderClause.?.map { _ getOrElse Nil }
    ~ windowFrameClause.?
    map BasicWindowSpec.tupled
    opaque "basic-window-specification"
  )

  private val refinedWindowSpecification: P[WindowSpec] =
    existingWindowName.map { WindowSpecRef(_) } ~ windowFrameClause.? map {
      case (ref, frame) => ref between frame
    } opaque "refined-window-specification"

  val windowSpecification: P[WindowSpec] = (
    "(" ~ (refinedWindowSpecification | basicWindowSpecification) ~ ")"
    opaque "window-specification"
  )

  private val windowDefinition: P[LogicalPlan => WindowDef] =
    windowName ~ AS ~ windowSpecification map {
      case (name, spec) => let(name, spec) _
    } opaque "window-definition"

  private val windowDefinitionList: P[LogicalPlan => WindowDef] =
    windowDefinition rep (min = 1, sep = ",") map {
      _ reduce { _ compose _ }
    } opaque "window-definition-list"

  val windowClause: P[LogicalPlan => LogicalPlan] =
    WINDOW ~ windowDefinitionList opaque "window-clause"
}

// SQL06 section 7.12
object QuerySpecificationParser extends LoggingParser {
  import AggregateFunctionParser._
  import GroupByClauseParser._
  import IdentifierParser._
  import KeywordParser._
  import NameParser._
  import NumericParser._
  import OrderByClauseParser._
  import SearchConditionParser._
  import TableReferenceParser._
  import ValueExpressionParser._
  import WhitespaceApi._
  import WindowClauseParser._

  private val asClause: P[Name] =
    AS.? ~ columnName opaque "as-clause"

  private val derivedColumn: P[NamedExpression] = (
    (valueExpression ~ asClause).map { case (e, a) => e as a }
    | valueExpression.map { NamedExpression.apply }
    opaque "derived-column"
  )

  private val asteriskedIdentifier: P[Name] =
    identifier opaque "asterisked-identifier"

  private val asteriskedIdentifierChain: P[Seq[Name]] =
    asteriskedIdentifier rep (min = 1, sep = ".") opaque "asterisked-identifier-chain"

  val qualifiedAsterisk: P[NamedExpression] = (
    asteriskedIdentifierChain ~ "." ~ "*"
    map { case Seq(qualifier) => * of qualifier }
    opaque "qualified-asterisk"
  )

  private val selectSublist: P[NamedExpression] = (
    qualifiedAsterisk
    | derivedColumn
    opaque "select-sublist"
  )

  private val selectList: P[Seq[NamedExpression]] = (
    ("*" attach * :: Nil)
    | selectSublist.rep(min = 1, sep = ",")
    opaque "select-list"
  )

  private val tableReferenceList: P[LogicalPlan] = (
    tableReference
    rep (min = 1, sep = ",")
    map { _ reduce { _ join _ } }
    opaque "table-reference-list"
  )

  private val fromClause: P[LogicalPlan] =
    FROM ~ tableReferenceList opaque "from-clause"

  private val whereClause: P[LogicalPlan => LogicalPlan] = (
    WHERE ~ searchCondition
    map { condition => (_: LogicalPlan) filter condition }
    opaque "where-clause"
  )

  private val havingClause: P[LogicalPlan => LogicalPlan] = (
    HAVING ~ searchCondition
    map { condition => (_: LogicalPlan) filter condition }
    opaque "having-clause"
  )

  @ExtendedSQLSyntax
  private val limitClause: P[LogicalPlan => LogicalPlan] = (
    LIMIT ~ unsignedInteger filter { _.isValidInt }
    map { n => (_: LogicalPlan) limit n.toInt }
    opaque "limit-clause"
  )

  @ExtendedSQLSyntax
  val querySpecification: P[LogicalPlan] = (
    SELECT
    ~ setQuantifier.?.map { _.orIdentity }
    ~ selectList
    ~ fromClause.?.map { _ getOrElse SingleRowRelation() }
    ~ whereClause.?.map { _.orIdentity }
    ~ groupByClause.?
    ~ havingClause.?
    ~ windowClause.?.map { _.orIdentity }
    ~ orderByClause.?.map { _.orIdentity }
    ~ limitClause.?.map { _.orIdentity } map {
      case (distinct, output, from, where, maybeGroupBy, maybeHaving, window, orderBy, limit) =>
        // Parses queries containing GROUP BY, HAVING, or both as aggregations. If a HAVING clause
        // exists without a GROUP BY clause, the grouping key list should be empty (i.e., global
        // aggregation).
        val maybeGroupingKeys = maybeGroupBy orElse maybeHaving.map { _ => Nil }

        val select = maybeGroupingKeys map { keys =>
          (_: LogicalPlan) groupBy keys agg output
        } getOrElse {
          // Queries with neither HAVING nor GROUP BY are always parsed as simple projections.
          // However, some of them may actually be global aggregations due to aggregate functions
          // appearing in `SELECT` and/or `ORDER BY` clauses. E.g.:
          //
          //  - SELECT count(*) FROM t
          //  - SELECT count(count(*)) OVER () FROM t
          //  - SELECT 1 FROM t ORDER BY count(*)
          //  - SELECT 1 FROM t ORDER BY count(count(*)) OVER ()
          //
          // This is because we cannot recognize aggregate functions during the parsing phase. We
          // will rewrite these queries into global aggregations during the analysis phase, though.
          (_: LogicalPlan) select output
        }

        val having = maybeHaving.orIdentity

        limit
          .compose(orderBy)
          .compose(window)
          .compose(having)
          .compose(distinct)
          .compose(select)
          .compose(where)
          .apply(from)
    }
  ) opaque "query-specification"
}

// SQL06 section 7.13
object QueryExpressionParser extends LoggingParser {
  import KeywordParser._
  import NameParser._
  import QuerySpecificationParser._
  import TableReferenceParser._
  import WhitespaceApi._

  private val withColumnList: P[Seq[Name]] = columnNameList opaque "with-column-list"

  private val withListElement: P[(Name, LogicalPlan => LogicalPlan)] =
    queryName ~ ("(" ~ withColumnList ~ ")").? ~ AS ~ "(" ~ P(queryExpression) ~ ")" map {
      case (name, Some(columns), query) => name -> let(name, query rename columns) _
      case (name, None, query)          => name -> let(name, query) _
    } opaque "with-list-element"

  private def checkDuplicatedNames(names: Seq[Name]): Unit = names groupBy identity foreach {
    case (name, group) if group.length > 1 =>
      throw new AnalysisException(s"WITH query name $name specified more than once")

    case _ =>
  }

  private val withList: P[LogicalPlan => LogicalPlan] =
    withListElement rep (min = 1, sep = ",") map { pairs =>
      val (names, builders) = pairs.unzip
      checkDuplicatedNames(names)
      builders reduce { _ compose _ }
    } opaque "with-list"

  private val withClause: P[LogicalPlan => LogicalPlan] = WITH ~ withList opaque "with-clause"

  private val simpleTable: P[LogicalPlan] = querySpecification opaque "query-specification"

  private val queryPrimary: P[LogicalPlan] = (
    "(" ~ P(queryExpressionBody) ~ ")"
    | simpleTable
    opaque "query-primary"
  )

  private val queryTerm: P[LogicalPlan] = {
    val intersect = (_: LogicalPlan) intersect (_: LogicalPlan)
    queryPrimary chain (INTERSECT attach intersect) opaque "query-term"
  }

  private lazy val queryExpressionBody: P[LogicalPlan] = {
    val union = (_: LogicalPlan) union (_: LogicalPlan)
    val except = (_: LogicalPlan) except (_: LogicalPlan)

    queryTerm chain (
      (UNION ~ ALL.? attach union) | (EXCEPT attach except)
    ) opaque "query-expression-body"
  }

  lazy val queryExpression: P[LogicalPlan] = withClause.? ~ queryExpressionBody map {
    case (cte, query) => cte.orIdentity apply query
  } opaque "query-expression"
}

// SQL06 section 7.15
object SubqueryParser extends LoggingParser {
  import QueryExpressionParser._
  import WhitespaceApi._

  private val subquery: P[LogicalPlan] = "(" ~ P(queryExpression) ~ ")" opaque "subquery"

  val tableSubquery: P[LogicalPlan] = subquery opaque "table-subquery"
}

object OrderByClauseParser extends LoggingParser {
  import KeywordParser._
  import SortSpecificationListParser._
  import WhitespaceApi._

  val orderByClause: P[LogicalPlan => LogicalPlan] =
    ORDER ~ BY ~ sortSpecificationList map { sortOrders =>
      // Note that `orderBy` builds an `UnresolvedSort` plan node, which is dedicated for
      // representing SQL `ORDER BY` clauses.
      (_: LogicalPlan) orderBy sortOrders
    } opaque "order-by-clause"
}

object SortSpecificationListParser extends LoggingParser {
  import KeywordParser._
  import ValueExpressionParser._
  import WhitespaceApi._

  private val orderingSpecification: P[Expression => SortOrder] = (
    ASC.attach { (_: Expression).asc }
    | DESC.attach { (_: Expression).desc }
    opaque "ordering-specification"
  )

  private val nullOrdering: P[SortOrder => SortOrder] =
    NULLS ~ (
      FIRST.attach { (_: SortOrder).nullsFirst }
      | LAST.attach { (_: SortOrder).nullsLast }
    ) opaque "null-ordering"

  private val sortKey: P[Expression] = valueExpression opaque "sort-key"

  private val sortSpecification: P[SortOrder] = (
    sortKey
    ~ orderingSpecification.?.map { _ getOrElse { (_: Expression).asc } }
    ~ nullOrdering.?.map { _ getOrElse { (_: SortOrder).nullsLarger } }
    map { case (key, f, g) => f andThen g apply key }
    opaque "sort-specification"
  )

  val sortSpecificationList: P[Seq[SortOrder]] =
    sortSpecification rep (min = 1, sep = ",") opaque "sort-specification-list"
}
