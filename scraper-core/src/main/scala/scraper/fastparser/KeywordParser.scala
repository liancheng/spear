package scraper.fastparser

import scala.collection.mutable

import fastparse.all._

// SQL06 section 5.2
object KeywordParser {
  private val reservedWords: mutable.ArrayBuffer[P0] = mutable.ArrayBuffer.empty[P0]

  private def mkKeyword(literal: String): P0 = {
    val parser = IgnoreCase(literal) opaque literal.toUpperCase
    reservedWords += parser
    parser
  }

  val ALL: P0 = mkKeyword("ALL")
  val AND: P0 = mkKeyword("AND")
  val AS: P0 = mkKeyword("AS")
  val BY: P0 = mkKeyword("BY")
  val DATE: P0 = mkKeyword("DATE")
  val DAY: P0 = mkKeyword("DAY")
  val DISTINCT: P0 = mkKeyword("DISTINCT")
  val FALSE: P0 = mkKeyword("FALSE")
  val FROM: P0 = mkKeyword("FROM")
  val GROUP: P0 = mkKeyword("GROUP")
  val HAVING: P0 = mkKeyword("HAVING")
  val HOUR: P0 = mkKeyword("HOUR")
  val INTERVAL: P0 = mkKeyword("INTERVAL")
  val IS: P0 = mkKeyword("IS")
  val MINUTE: P0 = mkKeyword("MINUTE")
  val MODULE: P0 = mkKeyword("MODULE")
  val MONTH: P0 = mkKeyword("MONTH")
  val NOT: P0 = mkKeyword("NOT")
  val OR: P0 = mkKeyword("OR")
  val SECOND: P0 = mkKeyword("SECOND")
  val SELECT: P0 = mkKeyword("SELECT")
  val TIME: P0 = mkKeyword("TIME")
  val TIMESTAMP: P0 = mkKeyword("TIMESTAMP")
  val TO: P0 = mkKeyword("TO")
  val TRUE: P0 = mkKeyword("TRUE")
  val UESCAPE: P0 = mkKeyword("UESCAPE")
  val UNKNOWN: P0 = mkKeyword("UNKOWN")
  val WHERE: P0 = mkKeyword("WHERE")
  val YEAR: P0 = mkKeyword("YEAR")

  val keyword: P0 = reservedWords reduce (_ | _) opaque "keyword"
}
