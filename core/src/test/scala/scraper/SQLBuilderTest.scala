package scraper

import scala.util.control.NonFatal

import org.scalatest.BeforeAndAfterAll

import scraper.expressions.Expression

abstract class SQLBuilderTest
  extends LoggingFunSuite with TestUtils with BeforeAndAfterAll {

  protected def checkSQL(e: Expression, expectedSQL: String): Unit = {
    try {
      assert(e.sql.get === expectedSQL)
    } catch {
      case NonFatal(cause) =>
        fail(
          s"""Wrong SQL generated for the following expression:
             |${e.prettyTree}
             |
             |Expected: $expectedSQL
             |Actual:   ${e.sql.get}
             |""".stripMargin,
          cause
        )
    }
  }
}
