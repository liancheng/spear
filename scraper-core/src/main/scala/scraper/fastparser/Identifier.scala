package scraper.fastparser

import java.lang.Character._

import fastparse.all._

// SQL06 section 5.2 and 5.4
object Identifier {
  import Keyword._
  import Symbol._
  import Separator.whitespace
  import WhitespaceApi._

  val regularIdentifier: P[String] = {
    val identifierStart: P0 = {
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
      }
    }

    val identifierExtend: P0 = {
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
      }
    }

    val identifierPart: P0 = identifierStart | identifierExtend
    val identifierBody: P0 = identifierStart ~~ identifierPart.repX

    identifierBody.!
  }

  private val delimitedIdentifierPart: P[Char] = (`""` | (!`"` ~~ AnyChar)).c1

  val delimitedIdentifier: P[String] = {
    val delimitedIdentifierBody: P[String] = delimitedIdentifierPart.repX.map(_.mkString)
    `"` ~~ delimitedIdentifierBody ~~ `"`
  }

  private val hexit: P0 = CharIn('A' to 'F', 'a' to 'f', '0' to '9')

  val unicodeDelimitedIdentifier: P[String] = {
    val unicodeIdentifierPart: P[Char] = delimitedIdentifierPart
    val unicodeDelimiterBody: P[String] = unicodeIdentifierPart rep 1 map (_.mkString)

    val unicodeEscapeCharacter: P0 = !(hexit | "+" | `'` | `"` | whitespace) ~~ AnyChar
    val unicodeEscapeSpecifier: P[Char] = (
      UESCAPE ~ (`'` ~~ unicodeEscapeCharacter.c1 ~~ `'`)
    ).? map (_ getOrElse '\\')

    def parseUnicodeIdentifier(body: String, uescape: Char): String = {
      def hex(digits: Int): P[Char] = hexit.repX(min = digits, max = digits).! map {
        Integer.parseUnsignedInt(_, 16).toChar
      }

      val unicodeEscapeCharacter: P0 = uescape.toString
      val unicodeCharacterEscapeValue: P[Char] = unicodeEscapeCharacter.repX(min = 2, max = 2).c1

      val unicode4DigitEscapeValue: P[Char] = unicodeEscapeCharacter.~/ ~~ hex(4)
      val unicode6DigitEscapeValue: P[Char] = unicodeEscapeCharacter ~~ "+".~/ ~~ hex(6)

      val unicodeEscapeValue: P[Char] = (
        unicodeCharacterEscapeValue
        | unicode6DigitEscapeValue
        | unicode4DigitEscapeValue
      )

      (unicodeEscapeValue | delimitedIdentifierPart repX 1 map (_.mkString) parse body).get.value
    }

    ("U&" ~~ `"`).~/ ~~ unicodeDelimiterBody ~~ `"` ~ unicodeEscapeSpecifier map {
      case (body, uescape) => parseUnicodeIdentifier(body, uescape)
    }
  }

  val identifier: P[String] = !keyword ~~ (
    unicodeDelimitedIdentifier
    | regularIdentifier
    | delimitedIdentifier
  )
}
