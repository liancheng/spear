package spear

import scala.util.Try

package object utils {
  def trySequence[T](seq: Seq[Try[T]]): Try[Seq[T]] = seq match {
    case xs if xs.isEmpty => Try(Nil)
    case Seq(x, xs @ _*)  => for (head <- x; tail <- trySequence(xs)) yield head +: tail
  }
}
