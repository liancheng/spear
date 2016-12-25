package scraper

import fastparse.WhitespaceApi.Wrapper

// SQL06 section 5.2
package object fastparser {
  import fastparse.all._

  val WhitespaceApi: Wrapper = Wrapper(Separator.separator.rep)

  implicit class CaptureSingleChar(parser: P0) {
    /** Captures the 1st character of the text parsed by `parser` as a `Char`. */
    def c1: P[Char] = parser.! map (_.head)
  }
}
