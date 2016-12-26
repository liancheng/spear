package scraper.fastparser

import fastparse.all._

import scraper.Name

object QuerySpecificationParser {
  import IdentifierParser._
  import KeywordParser._
  import SymbolParser._
  import ValueExpression._
  import WhitespaceApi._

  val setQuantifier: P0 =
    DISTINCT | ALL opaque "set-quantifier"

  val asClause: P0 =
    AS ~ columnName.X opaque "as-clause"

  val derivedColumn: P0 =
    valueExpression ~ asClause.? opaque "derived-column"

  val asteriskedIdentifier: P0 =
    identifier.X opaque "asterisked-identifier"

  val asteriskedIdentifierChain: P0 =
    asteriskedIdentifier rep (min = 1, sep = ".") opaque "asterisked-identifier-chain"

  // SQL06 section 6.3
  val valueExpressionPrimary: P0 =
    ???

  val columnNameList: P[Seq[Name]] =
    columnName rep (min = 1, sep = ",") opaque "column-name-list"

  val allFieldsColumnNameList: P[Seq[Name]] =
    columnNameList opaque "all-fields-column-name-list"

  val allFieldsReference: P0 = (
    valueExpressionPrimary ~ "." ~ `*`
    ~ (AS ~ "(" ~ allFieldsColumnNameList.X ~ ")").?
  ) opaque "all-fields-reference"

  val qualifiedAsterisk: P0 =
    asteriskedIdentifierChain ~ "." ~ `*` | allFieldsReference opaque "qualified-asterisk"

  val selectSublist: P0 =
    derivedColumn | qualifiedAsterisk opaque "select-sublist"

  val selectList: P0 =
    `*` | selectSublist rep (min = 1, sep = ",") opaque "select-list"

  val tableOrQueryName: P0 =
    tableName.X opaque "table-or-query-name"

  val tablePrimary: P0 =
    tableOrQueryName opaque "table-primary"

  val tableFactor: P0 =
    tablePrimary opaque "table-factor"

  // SQL06 section 7.6
  val tableReference: P0 =
    tableFactor opaque "table-reference"

  val tableReferenceList: P0 =
    tableReference rep (min = 1, sep = ",") opaque "table-reference-list"

  // SQL06 section 7.5
  val fromClause: P0 =
    FROM ~ tableReferenceList opaque "from-clause"

  val whereClause: P0 =
    ???

  // SQL06 section 7.4
  val tableExpression: P0 = (
    fromClause
    ~ whereClause.?
  ) opaque "table-expression"

  // SQL06 section 7.12
  val querySpecification: P0 =
    SELECT ~ setQuantifier.? ~ selectList ~ tableExpression opaque "query-specification"
}
