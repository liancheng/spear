package spear.parsers

trait ParserImplicits[+T] {
  import fastparse.all.{P, Parser}

  val self: Parser[T]

  def fold[U >: T](sep: => P[(U, U) => U], min: Int = 0): P[U] = {
    import WhitespaceApi.parserApi

    self ~ (sep ~ self rep (min = min)) map {
      case (head, tail) =>
        tail.foldLeft(head: U) {
          case (lhs, (op, rhs)) =>
            op(lhs, rhs)
        }
    }
  }
}
