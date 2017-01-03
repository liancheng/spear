package scraper

import fastparse.WhitespaceApi.Wrapper

// SQL06 section 5.2
package object fastparser {
  import fastparse.all._

  val WhitespaceApi: Wrapper = Wrapper(SeparatorParser.separator.rep)

  implicit class ExtendedDSL[+T](parser: P[T]) {
    /** Captures the 1st character of the text parsed by `parser` as a `Char`. */
    def char: P[Char] = parser.! map (_.head)

    /** Drops the semantic value of `parser` and convert `parser` to a `P0`. */
    def drop: P0 = parser map (_ => ())

    def chain[U >: T](sep: => P[(U, U) => U]): P[U] = {
      import WhitespaceApi._

      parser ~ (sep ~ parser).rep map {
        case (first, list) =>
          list.foldLeft(first: U) {
            case (lhs, (op, rhs)) =>
              op(lhs, rhs)
          }
      }
    }

    def attach[U](value: => U): P[U] = parser map { _ => value }
  }
}
