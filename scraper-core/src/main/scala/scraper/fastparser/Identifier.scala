package scraper.fastparser

import java.lang.Character._

import fastparse.all._

// SQL06 section 5.2 and 5.4
object Identifier {
  import Keyword._
  import Symbol._
  import Separator.whitespace

  val identifier: P0 = {
    val delimitedIdentifierPart: P0 = !`"` | `""`

    val delimitedIdentifier: P0 = {
      val delimitedIdentifierBody: P0 = delimitedIdentifierPart.rep
      `"` ~ delimitedIdentifierBody ~ `"`
    }

    val regularIdentifier: P0 = {
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
      val identifierBody: P0 = identifierStart ~ identifierPart.rep

      identifierBody
    }

    val unicodeDelimitedIdentifier: P0 = {
      val hexit: P0 = CharIn('A' to 'F', 'a' to 'f')
      val unicodeEscapeCharacter: P0 = !(hexit | "+" | `'` | `"` | whitespace)

      val unicode4DigitEscapeValue: P0 = unicodeEscapeCharacter ~ hexit.rep(exactly = 4)
      val unicode6DigitEscapeValue: P0 = unicodeEscapeCharacter ~ "+" ~ hexit.rep(exactly = 6)
      val unicodeCharacterEscapeValue: P0 = unicodeEscapeCharacter.rep(exactly = 2)

      val unicodeEscapeValue: P0 = (
        unicode4DigitEscapeValue
        | unicode6DigitEscapeValue
        | unicodeCharacterEscapeValue
      )

      val unicodeIdentifierPart: P0 = delimitedIdentifierPart | unicodeEscapeValue
      val unicodeDelimiterBody: P0 = unicodeIdentifierPart rep 1
      val unicodeEscapeSpecifier: P0 = (UNESCAPE ~ `'` ~ unicodeEscapeCharacter ~ `'`).?

      "U&" ~ `"` ~ unicodeDelimiterBody ~ `"` ~ unicodeEscapeSpecifier
    }

    !keyword ~ (regularIdentifier | delimitedIdentifier | unicodeDelimitedIdentifier)
  }
}
