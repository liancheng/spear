package scraper.fastparser

import scala.collection.mutable

import fastparse.all._

// SQL06 section 5.2
object Keyword {
  private val reservedWords: mutable.ArrayBuffer[P0] = mutable.ArrayBuffer.empty[P0]

  private def mkKeyword(literal: String): P0 = {
    val parser = IgnoreCase(literal)
    reservedWords += parser
    parser
  }

  val ALL: P0 = mkKeyword("ALL")
  val BY: P0 = mkKeyword("BY")
  val DATE: P0 = mkKeyword("DATE")
  val DAY: P0 = mkKeyword("DAY")
  val DISTINCT: P0 = mkKeyword("DISTINCT")
  val FROM: P0 = mkKeyword("FROM")
  val GROUP: P0 = mkKeyword("GROUP")
  val HOUR: P0 = mkKeyword("HOUR")
  val INTERVAL: P0 = mkKeyword("INTERVAL")
  val MINUTE: P0 = mkKeyword("MINUTE")
  val MONTH: P0 = mkKeyword("MONTH")
  val SECOND: P0 = mkKeyword("SECOND")
  val SELECT: P0 = mkKeyword("SELECT")
  val TIME: P0 = mkKeyword("TIME")
  val TIMESTAMP: P0 = mkKeyword("TIMESTAMP")
  val TO: P0 = mkKeyword("TO")
  val UNESCAPE: P0 = mkKeyword("UNESCAPE")
  val WHERE: P0 = mkKeyword("WHERE")
  val YEAR: P0 = mkKeyword("YEAR")

  val keyword: P0 = reservedWords.reduce(_ | _)
}
