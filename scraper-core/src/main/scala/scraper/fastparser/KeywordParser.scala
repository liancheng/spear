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
  val ARRAY: P0 = mkKeyword("ARRAY")
  val AS: P0 = mkKeyword("AS")
  val ASC: P0 = mkKeyword("ASC")
  val BIGINT: P0 = mkKeyword("BIGINT")
  val BINARY: P0 = mkKeyword("BINARY")
  val BLOB: P0 = mkKeyword("BLOB")
  val BOOLEAN: P0 = mkKeyword("BOOLEAN")
  val BY: P0 = mkKeyword("BY")
  val DATE: P0 = mkKeyword("DATE")
  val DAY: P0 = mkKeyword("DAY")
  val DEC: P0 = mkKeyword("DEC")
  val DECIMAL: P0 = mkKeyword("DECIMAL")
  val DESC: P0 = mkKeyword("DESC")
  val DISTINCT: P0 = mkKeyword("DISTINCT")
  val DOUBLE: P0 = mkKeyword("DOUBLE")
  val EXCEPT: P0 = mkKeyword("EXCEPT")
  val FALSE: P0 = mkKeyword("FALSE")
  val FIRST: P0 = mkKeyword("FIRST")
  val FLOAT: P0 = mkKeyword("FLOAT")
  val FROM: P0 = mkKeyword("FROM")
  val GROUP: P0 = mkKeyword("GROUP")
  val HAVING: P0 = mkKeyword("HAVING")
  val HOUR: P0 = mkKeyword("HOUR")
  val INT: P0 = mkKeyword("INT")
  val INTEGER: P0 = mkKeyword("INTEGER")
  val INTERSECT: P0 = mkKeyword("INTERSECT")
  val INTERVAL: P0 = mkKeyword("INTERVAL")
  val IS: P0 = mkKeyword("IS")
  val LARGE: P0 = mkKeyword("LARGE")
  val LAST: P0 = mkKeyword("LAST")
  val MINUTE: P0 = mkKeyword("MINUTE")
  val MODULE: P0 = mkKeyword("MODULE")
  val MONTH: P0 = mkKeyword("MONTH")
  val NOT: P0 = mkKeyword("NOT")
  val NULLS: P0 = mkKeyword("NULLS")
  val NUMERIC: P0 = mkKeyword("NUMERIC")
  val OBJECT: P0 = mkKeyword("OBJECT")
  val OR: P0 = mkKeyword("OR")
  val ORDER: P0 = mkKeyword("ORDER")
  val PRECISION: P0 = mkKeyword("PRECISION")
  val REAL: P0 = mkKeyword("REAL")
  val ROW: P0 = mkKeyword("ROW")
  val SECOND: P0 = mkKeyword("SECOND")
  val SELECT: P0 = mkKeyword("SELECT")
  val SMALLINT: P0 = mkKeyword("SMALLINT")
  val TIME: P0 = mkKeyword("TIME")
  val TIMESTAMP: P0 = mkKeyword("TIMESTAMP")
  val TO: P0 = mkKeyword("TO")
  val TRUE: P0 = mkKeyword("TRUE")
  val UESCAPE: P0 = mkKeyword("UESCAPE")
  val UNION: P0 = mkKeyword("UNION")
  val UNKNOWN: P0 = mkKeyword("UNKOWN")
  val WHERE: P0 = mkKeyword("WHERE")
  val WITH: P0 = mkKeyword("WITH")
  val WITHOUT: P0 = mkKeyword("WITHOUT")
  val YEAR: P0 = mkKeyword("YEAR")
  val ZONE: P0 = mkKeyword("ZONE")

  val keyword: P0 = reservedWords reduce { _ | _ } opaque "keyword"
}
