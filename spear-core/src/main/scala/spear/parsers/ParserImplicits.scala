package spear.parsers

trait ParserImplicits[+T] {
  import fastparse.all._

  val self: Parser[T]

  /** Captures the 1st character of the text parsed by `parser` as a `Char`. */
  def char: P[Char] = self.! map { _.head }

  /** Drops the semantic value of `self` and convert `self` to a `P0`. */
  def drop: P0 = self map { _ => () }

  def fold[U >: T](sep: => P[(U, U) => U], min: Int = 0): P[U] = {
    import WhitespaceApi._

    self ~ (sep ~ self rep (min = min)) map {
      case (head, tail) =>
        tail.foldLeft(head: U) {
          case (lhs, (op, rhs)) =>
            op(lhs, rhs)
        }
    }
  }

  /** Overwrites the original semantics value with a new `value`. */
  def ==>[U](value: => U): P[U] = self map { _ => value }
}
