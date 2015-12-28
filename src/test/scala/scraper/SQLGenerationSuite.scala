package scraper

import org.scalatest.BeforeAndAfterAll

import scraper.expressions.Literal.{False, True}
import scraper.expressions.dsl._
import scraper.expressions.{Expression, Literal}
import scraper.plans.logical.LogicalPlan
import scraper.types.TestUtils

class SQLGenerationSuite extends LoggingFunSuite with TestUtils with BeforeAndAfterAll {
  private val context = new LocalContext

  override protected def beforeAll(): Unit = {
    context range 10 select ('id as 'a) registerAsTable "t0"
    context range 10 select ('id as 'b) registerAsTable "t1"
  }

  override protected def afterAll(): Unit = {
    context.catalog.removeRelation("t0")
    context.catalog.removeRelation("t1")
  }

  private def checkSQL(e: Expression, expectedSQL: String): Unit = {
    val maybeSQL = e.sql

    if (maybeSQL.isEmpty) {
      fail(
        s"""Cannot convert the following expression to its SQL form:
           |
           |${e.prettyTree}
         """.stripMargin
      )
    }

    try {
      assert(maybeSQL.get === expectedSQL)
    } catch {
      case cause: Throwable =>
        fail(
          s"""Wrong SQL generated for the following expression:
             |
             |${e.prettyTree}
             |
             |$cause
           """.stripMargin
        )
    }
  }

  private def checkSQL(plan: LogicalPlan, expectedSQL: String): Unit = {
    val maybeSQL = plan.sql(context)

    if (maybeSQL.isEmpty) {
      fail(
        s"""Cannot convert the following logical query plan to SQL:
           |
           |${plan.prettyTree}
         """.stripMargin
      )
    }

    val actualSQL = maybeSQL.get

    try {
      assert(actualSQL === expectedSQL)
    } catch {
      case cause: Throwable =>
        fail(
          s"""Wrong SQL generated for the following logical query plan:
             |
             |${plan.prettyTree}
             |
             |$cause
           """.stripMargin
        )
    }

    checkDataFrame(context.q(actualSQL), new DataFrame(plan, context))
  }

  private def checkSQL(df: DataFrame, expectedSQL: String): Unit = {
    checkSQL(df.queryExecution.analyzedPlan, expectedSQL)
  }

  test("literals") {
    checkSQL(True, "true")
    checkSQL(False, "false")
    checkSQL(Literal("foo"), "\"foo\"")
    checkSQL(Literal("\"foo"), "\"\\\"foo\"")
    checkSQL(Literal(0), "0")
    checkSQL(Literal(1: Byte), "CAST(1 AS TINYINT)")
    checkSQL(Literal(2: Short), "CAST(2 AS SMALLINT)")
    checkSQL(Literal(4: Long), "CAST(4 AS BIGINT)")
    checkSQL(Literal(1.1F), "CAST(1.1 AS FLOAT)")
    checkSQL(Literal(1.2D), "CAST(1.2 AS DOUBLE)")
  }

  test("arithmetic expressions") {
    checkSQL('a + 'b, "(`a` + `b`)")
    checkSQL('a - 'b, "(`a` - `b`)")
    checkSQL('a * 'b, "(`a` * `b`)")
    checkSQL('a / 'b, "(`a` / `b`)")
    checkSQL(-'a, "(-`a`)")
    checkSQL(+'a, "(+`a`)")
  }

  test("single row project") {
    checkSQL(context single 1, "SELECT 1 AS `col0`")
    checkSQL(context single (1 as 'a), "SELECT 1 AS `a`")
  }

  test("project with limit") {
    checkSQL(context single 1 limit 1, "SELECT 1 AS `col0` LIMIT 1")
    checkSQL(context single (1 as 'a) limit 1, "SELECT 1 AS `a` LIMIT 1")
  }

  test("table lookup") {
    checkSQL(context table "t0", "SELECT `a` FROM `t0`")

    checkSQL(
      context table "t0" filter 'a > 3L,
      "SELECT `a` FROM `t0` WHERE (`a` > CAST(3 AS BIGINT))"
    )
  }

  test("join") {
    val t0 = context table "t0"
    val t1 = context table "t1"

    checkSQL(
      t0.join(t1),
      "SELECT `a`, `b` FROM `t0` INNER JOIN `t1`"
    )

    checkSQL(
      t0 join t1 on 'a =:= 'b,
      "SELECT `a`, `b` FROM `t0` INNER JOIN `t1` ON (`a` = `b`)"
    )
  }
}
