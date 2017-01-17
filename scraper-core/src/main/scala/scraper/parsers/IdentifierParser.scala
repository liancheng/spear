package scraper.parsers

import java.lang.Character._

import fastparse.all._

import scraper.Name

// SQL06 section 5.4
object IdentifierParser {
  import KeywordParser._
  import SeparatorParser.whitespace
  import WhitespaceApi._

  private val identifierStart: P0 = {
    val categories = Set(
      LOWERCASE_LETTER: Int,
      UPPERCASE_LETTER: Int,
      TITLECASE_LETTER: Int,
      MODIFIER_LETTER: Int,
      OTHER_LETTER: Int,
      LETTER_NUMBER: Int
    )

    CharPred { ch =>
      categories.contains(ch.getType)
    } opaque "identifier-start"
  }

  private val identifierExtend: P0 = {
    val MiddleDot = '\u00b7'
    val categories = Set(
      NON_SPACING_MARK: Int,
      COMBINING_SPACING_MARK: Int,
      DECIMAL_DIGIT_NUMBER: Int,
      CONNECTOR_PUNCTUATION: Int,
      FORMAT: Int
    )

    CharPred { ch =>
      (ch == MiddleDot) || (categories contains ch.getType)
    } opaque "identifier-extend"
  }

  private val identifierPart: P0 = identifierStart | identifierExtend opaque "identifier-part"

  private val identifierBody: P0 = identifierStart ~~ identifierPart.repX opaque "identifier-body"

  // SQL06 section 5.2
  val regularIdentifier: P[Name] =
    identifierBody.! map Name.caseInsensitive opaque "regular-identifier"

  private val delimitedIdentifierPart: P[Char] =
    ("\"\"" | (!"\"" ~~ AnyChar)).char opaque "delimiter-identifier-part"

  private val delimitedIdentifierBody: P[String] =
    delimitedIdentifierPart.repX map (_.mkString) opaque "delimited-identifier-body"

  // SQL06 section 5.2
  val delimitedIdentifier: P[Name] = (
    "\"" ~~ delimitedIdentifierBody ~~ "\""
    map Name.caseSensitive
    opaque "delimited-identifier"
  )

  private val hexit: P0 = CharIn('A' to 'F', 'a' to 'f', '0' to '9') opaque "hexit"

  private val unicodeIdentifierPart: P[Char] =
    delimitedIdentifierPart opaque "unicode-identifier-part"

  private val unicodeDelimiterBody: P[String] =
    unicodeIdentifierPart repX 1 map (_.mkString) opaque "unicode-delimiter-body"

  private val unicodeEscapeCharacter: P0 =
    !(hexit | "+" | "'" | "\"" | whitespace) ~~ AnyChar opaque "unicode-escape-character"

  val unicodeEscapeSpecifier: P[Char] = (
    (UESCAPE ~ ("'" ~~ unicodeEscapeCharacter.char ~~ "'")).?
    map (_ getOrElse '\\')
    opaque "unicode-escape-specifier"
  )

  def parseUnicodeRepresentations(body: String, uescape: Char): String = {
    def hex(digits: Int): P[Char] = hexit.repX(min = digits, max = digits).! map {
      Integer.parseInt(_, 16).toChar
    }

    val unicodeEscapeCharacter: P0 = uescape.toString opaque "unicode-escape-character"

    val unicodeCharacterEscapeValue: P[Char] =
      unicodeEscapeCharacter.repX(min = 2, max = 2).char opaque "unicode-character-escape-value"

    val unicode6DigitEscapeValue: P[Char] = (
      unicodeEscapeCharacter ~~ "+".~/ ~~ hex(6)
      opaque "unicode-6-digit-escape-value"
    )

    val unicode4DigitEscapeValue: P[Char] =
      unicodeEscapeCharacter.~/ ~~ hex(4) opaque "unicode-4-digit-escape-value"

    val unicodeEscapeValue: P[Char] = (
      unicodeCharacterEscapeValue
      | unicode6DigitEscapeValue
      | unicode4DigitEscapeValue
      opaque "unicode-escape-value"
    )

    val unicodeDelimiterBody = (
      unicodeEscapeValue | delimitedIdentifierPart
      repX 1
      map (_.mkString)
      opaque "unicode-delimiter-body"
    )

    unicodeDelimiterBody.parse(body).get.value
  }

  private val unicodeDelimitedIdentifier: P[Name] = (
    ("U&" ~~ "\"").~/ ~~ unicodeDelimiterBody ~~ "\"" ~ unicodeEscapeSpecifier
    map (parseUnicodeRepresentations _).tupled
    map Name.caseSensitive
    opaque "unicode-delimited-identifier"
  )

  private val actualIdentifier: P[Name] =
    unicodeDelimitedIdentifier | regularIdentifier | delimitedIdentifier opaque "actual-identifier"

  // SQL06 section 5.4
  val identifier: P[Name] = !keyword ~~ actualIdentifier opaque "identifier"

  // SQL06 section 5.2
  val qualifiedIdentifier: P[Name] = identifier opaque "qualified-identifier"
}

// SQL06 section 5.4
object IdentifierChainParser {
  import IdentifierParser._
  import WhitespaceApi._

  private val identifierChain: P[Seq[Name]] =
    identifier rep (min = 1, sep = ".") opaque "identifier-chain"

  val basicIdentifierChain: P[Seq[Name]] =
    identifierChain opaque "basic-identifier-chain"
}

// SQL06 section 5.4
object NameParser {
  import IdentifierParser._
  import WhitespaceApi._

  val columnName: P[Name] =
    identifier opaque "column-name"

  private val catalogName: P[Name] =
    identifier opaque "catalog-name"

  private val unqualifiedSchemaName: P[Name] =
    identifier opaque "unqualified-schema-name"

  case class SchemaName(schema: Name, catalog: Option[Name])

  private val schemaName: P[SchemaName] = (
    (catalogName ~ ".").? ~ unqualifiedSchemaName
    map { _.swap }
    map SchemaName.tupled
    opaque "schema-name"
  )

  private val localOrSchemaQualifier: P[SchemaName] =
    schemaName opaque "local-or-schema-qualifier"

  case class TableName(table: Name, schema: Option[SchemaName])

  private val localOrSchemaQualifiedName: P[TableName] = (
    (localOrSchemaQualifier ~ ".").? ~ qualifiedIdentifier
    map { _.swap }
    map TableName.tupled
    opaque "local-or-schema-qualified-name"
  )

  val tableName: P[TableName] =
    localOrSchemaQualifiedName opaque "table-name"

  val queryName: P[Name] =
    identifier opaque "query-name"

  val fieldName: P[Name] =
    identifier opaque "field-name"

  val functionName: P[Name] =
    identifier opaque "function-name"
}
