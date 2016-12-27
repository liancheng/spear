package scraper.fastparser

import fastparse.all._

import scraper.Name
import scraper.expressions.{AutoAlias, Expression, NamedExpression, Star}
import scraper.plans.logical._

// SQL06 section 7.6
object TableReferenceParser {
  import NameParser._
  import WhitespaceApi._

  private val tableOrQueryName: P[TableName] =
    tableName opaque "table-or-query-name"

  private val tablePrimary: P[TableName] =
    tableOrQueryName opaque "table-primary"

  private val tableFactor: P[TableName] =
    tablePrimary opaque "table-factor"

  lazy val tableReference: P[LogicalPlan] =
    tableFactor map { _.table } map table opaque "table-reference"
}

// SQL06 section 7.9
object GroupByClauseParser {
  import ColumnReferenceParser._
  import KeywordParser._
  import ValueExpressionParser._
  import WhitespaceApi._

  private val groupingColumnReference: P[Expression] =
    columnReference | valueExpression opaque "grouping-column-reference"

  private val groupingColumnReferenceList: P[Seq[Expression]] =
    groupingColumnReference rep (min = 1, sep = ",") opaque "grouping-column-reference-list"

  private val ordinaryGroupingSet: P[Seq[Expression]] = (
    groupingColumnReference.map { _ :: Nil }
    | "(" ~ groupingColumnReferenceList ~ ")"
    opaque "ordinary-grouping-set"
  )

  private val groupingElement: P[Seq[Expression]] =
    ordinaryGroupingSet opaque "grouping-element"

  private val groupingElementList: P[Seq[Expression]] = (
    groupingElement
    rep (min = 1, sep = ",")
    map { _.flatten }
    opaque "grouping-element-list"
  )

  lazy val groupByClause: P[Seq[Expression]] =
    GROUP ~ BY ~ groupingElementList opaque "group-by-clause"
}

// SQL06 section 7.12
object QuerySpecificationParser {
  import AggregateFunctionParser._
  import BooleanValueExpressionParser._
  import GroupByClauseParser._
  import IdentifierParser._
  import KeywordParser._
  import NameParser._
  import SymbolParser._
  import TableReferenceParser._
  import ValueExpressionParser._
  import WhitespaceApi._

  private val asClause: P[Name] =
    AS.? ~ columnName opaque "as-clause"

  private val derivedColumn: P[NamedExpression] = (
    (valueExpression ~ asClause).map { case (e, a) => e as a }
    | valueExpression.map { AutoAlias.named }
    opaque "derived-column"
  )

  private val asteriskedIdentifier: P[Name] =
    identifier opaque "asterisked-identifier"

  private val asteriskedIdentifierChain: P[Seq[Name]] =
    asteriskedIdentifier rep (min = 1, sep = ".") opaque "asterisked-identifier-chain"

  private val qualifiedAsterisk: P[Seq[Name]] =
    asteriskedIdentifierChain ~ "." ~ `*` opaque "qualified-asterisk"

  private val selectSublist: P[NamedExpression] = (
    qualifiedAsterisk.map { case Seq(qualifier) => Star(Some(qualifier)) }
    | derivedColumn
    opaque "select-sublist"
  )

  private val selectList: P[Seq[NamedExpression]] = (
    `*`.!.map { _ => Star(None) :: Nil }
    | selectSublist.rep(min = 1, sep = ",")
    opaque "select-list"
  )

  private val tableReferenceList: P[LogicalPlan] = (
    tableReference
    rep (min = 1, sep = ",")
    map { _ reduce (_ join _) }
    opaque "table-reference-list"
  )

  private val fromClause: P[LogicalPlan] =
    FROM ~ tableReferenceList opaque "from-clause"

  private val searchCondition: P[Expression] =
    booleanValueExpression opaque "search-condition"

  private val whereClause: P[LogicalPlan => LogicalPlan] = (
    (WHERE ~ searchCondition)
    map { cond => (_: LogicalPlan) filter cond }
    opaque "where-clause"
  )

  private val havingClause: P[LogicalPlan => LogicalPlan] = (
    (HAVING ~ searchCondition)
    map { cond => (_: LogicalPlan) filter cond }
    opaque "having-clause"
  )

  private val id: LogicalPlan => LogicalPlan = identity[LogicalPlan]

  lazy val querySpecification: P[LogicalPlan] = (
    SELECT
    ~ setQuantifier.?.map { _ getOrElse id }
    ~ selectList
    ~ fromClause.?.map { _ getOrElse SingleRowRelation }
    ~ whereClause.?.map { _ getOrElse id }
    ~ groupByClause.?
    ~ havingClause.?.map { _ getOrElse id } map {
      case (quantify, projectList, relation, filter, maybeGroups, having) =>
        val projectOrAgg = maybeGroups map { groups =>
          (_: LogicalPlan) groupBy groups agg projectList
        } getOrElse {
          (_: LogicalPlan) select projectList
        }

        filter andThen projectOrAgg andThen quantify andThen having apply relation
    }
    opaque "query-specification"
  )
}
