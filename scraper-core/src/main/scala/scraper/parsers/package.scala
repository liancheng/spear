package scraper

import scala.language.implicitConversions

import fastparse.WhitespaceApi.Wrapper

// SQL06 section 5.2
package object parsers {
  import fastparse.all._

  val WhitespaceApi: Wrapper = Wrapper(SeparatorParser.separator.rep)

  implicit def `Parser->ParserImplicits`[T](parser: Parser[T]): ParserImplicits[T] =
    new ParserImplicits[T] { override val self: Parser[T] = parser }

  implicit def `String->ParserImplicits`(literal: String): ParserImplicits[Unit] =
    new ParserImplicits[Unit] { override val self: Parser[Unit] = P(literal) }

  implicit class OrIdentity[A](maybeTransform: Option[A => A]) {
    def orIdentity: A => A = maybeTransform getOrElse identity[A] _
  }
}
