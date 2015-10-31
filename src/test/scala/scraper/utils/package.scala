package scraper

import scala.language.postfixOps

package object utils {
  private[scraper] def sideBySide(lhs: String, rhs: String, withHeader: Boolean): String = {
    val lhsLines = lhs split "\n"
    val rhsLines = rhs split "\n"

    val lhsWidth = lhsLines map (_.length) max
    val rhsWidth = rhsLines map (_.length) max

    val height = lhsLines.length max rhsLines.length
    val paddedLhs = lhsLines map (_ padTo (lhsWidth, ' ')) padTo (height, " " * lhsWidth)
    val paddedRhs = rhsLines map (_ padTo (rhsWidth, ' ')) padTo (height, " " * rhsWidth)

    val zipped = paddedLhs zip paddedRhs
    val header = if (withHeader) zipped take 1 else Array.empty[(String, String)]
    val contents = if (withHeader) zipped drop 1 else zipped

    def rtrim(str: String): String = str.replaceAll("\\s+$", "")

    header.map {
      case (lhsLine, rhsLine) =>
        rtrim(s"# $lhsLine   $rhsLine")
    } ++ contents.map {
      case (lhsLine, rhsLine) =>
        val diffIndicator = if (rtrim(lhsLine) != rtrim(rhsLine)) ">" else " "
        rtrim(s"$diffIndicator $lhsLine   $rhsLine")
    }
  } mkString "\n"
}
