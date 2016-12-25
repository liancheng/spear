package scraper.fastparser

import java.lang.Character._

import fastparse.all._

// SQL06 section 5.2 and 5.4
object Identifier {
  import Keyword._
  import Symbol._
  import Separator.whitespace
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

  private val identifierPart: P0 =
    identifierStart | identifierExtend opaque "identifier-part"

  private val identifierBody: P0 =
    identifierStart ~~ identifierPart.repX opaque "identifier-body"

  val regularIdentifier: P[String] =
    identifierBody.! opaque "regular-identifier"

  private val delimitedIdentifierPart: P[Char] =
    (`""` | (!`"` ~~ AnyChar)).char opaque "delimiter-identifier-part"

  private val delimitedIdentifierBody: P[String] =
    delimitedIdentifierPart.repX.map(_.mkString)

  val delimitedIdentifier: P[String] =
    `"` ~~ delimitedIdentifierBody ~~ `"` opaque "delimited-identifier"

  private val hexit: P0 =
    CharIn('A' to 'F', 'a' to 'f', '0' to '9') opaque "hexit"

  private val unicodeIdentifierPart: P[Char] =
    delimitedIdentifierPart opaque "unicode-identifier-part"

  private val unicodeDelimiterBody: P[String] =
    unicodeIdentifierPart repX 1 map (_.mkString) opaque "unicode-delimiter-body"

  private val unicodeEscapeCharacter: P0 =
    !(hexit | "+" | `'` | `"` | whitespace) ~~ AnyChar opaque "unicode-escape-character"

  private val unicodeEscapeSpecifier: P[Char] = (
    (UESCAPE ~ (`'` ~~ unicodeEscapeCharacter.char ~~ `'`)).?
    map (_ getOrElse '\\')
    opaque "unicode-escape-specifier"
  )

  private def parseUnicodeIdentifier(body: String, uescape: Char): String = {
    def hex(digits: Int): P[Char] = hexit.repX(min = digits, max = digits).! map {
      Integer.parseInt(_, 16).toChar
    }

    val unicodeEscapeCharacter: P0 =
      uescape.toString opaque "unicode-escape-character"

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
    ) opaque "unicode-escape-value"

    (unicodeEscapeValue | delimitedIdentifierPart repX 1 map (_.mkString) parse body).get.value
  }

  val unicodeDelimitedIdentifier: P[String] =
    ("U&" ~~ `"`).~/ ~~ unicodeDelimiterBody ~~ `"` ~ unicodeEscapeSpecifier map {
      case (body, uescape) => parseUnicodeIdentifier(body, uescape)
    } opaque "unicode-delimited-identifier"

  val identifier: P[String] = !keyword ~~ (
    unicodeDelimitedIdentifier
    | regularIdentifier
    | delimitedIdentifier
  ) opaque "identifier"
}
