package scraper

import scala.util.control.NonFatal

import org.scalatest.BeforeAndAfterAll

import scraper.expressions.Expression
import scraper.expressions.dsl._

abstract class SQLBuilderTest
  extends LoggingFunSuite with TestUtils with BeforeAndAfterAll {

  protected val context = new LocalContext(Test.defaultSettings)

  override protected def beforeAll(): Unit = {
    context range 10 select ('id as 'a) asTable "t0"
    context range 10 select ('id as 'b) asTable "t1"
  }

  override protected def afterAll(): Unit = {
    context.catalog removeRelation "t0"
    context.catalog removeRelation "t1"
  }

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
