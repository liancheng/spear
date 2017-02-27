package scraper

import scala.util.Try

package object utils {
  def sideBySide(lhs: String, rhs: String, withHeader: Boolean): String = {
    val lhsLines = lhs split "\n"
    val rhsLines = rhs split "\n"

    val lhsWidth = lhsLines.map { _.length }.max
    val rhsWidth = rhsLines.map { _.length }.max

    val height = lhsLines.length max rhsLines.length
    val paddedLhs = lhsLines map { _ padTo (lhsWidth, ' ') } padTo (height, " " * lhsWidth)
    val paddedRhs = rhsLines map { _ padTo (rhsWidth, ' ') } padTo (height, " " * rhsWidth)

    val zipped = paddedLhs zip paddedRhs
    val header = if (withHeader) zipped.headOption else None
    val contents = if (withHeader) zipped.tail else zipped

    def trimRight(str: String): String = str.replaceAll("\\s+$", "")

    val pipe = "\u2502"

    header.map {
      case (lhsLine, rhsLine) =>
        trimRight(s"# $lhsLine # $rhsLine")
    } ++ contents.map {
      case (lhsLine, rhsLine) =>
        val diffIndicator = if (trimRight(lhsLine) != trimRight(rhsLine)) "!" else " "
        trimRight(s"$diffIndicator $lhsLine $pipe $rhsLine")
    }
  } mkString ("\n", "\n", "")

  implicit class OneLineString(string: String) {
    def oneLine: String = oneLine('|', " ")

    def oneLine(joiner: String): String = oneLine('|', joiner)

    def oneLine(marginChar: Char, joiner: String): String =
      ((string stripMargin marginChar).lines mkString joiner).trim
  }

  def trySequence[T](seq: Seq[Try[T]]): Try[Seq[T]] = seq match {
    case xs if xs.isEmpty => Try(Nil)
    case Seq(x, xs @ _*)  => for (head <- x; tail <- trySequence(xs)) yield head +: tail
  }

  def quote(string: String): String = "'" + string.replace("\\", "\\\\").replace("'", "\\'") + "'"
}
