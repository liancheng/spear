package scraper

import org.scalatest.BeforeAndAfterAll

import scraper.expressions.Expression
import scraper.expressions.dsl._
import scraper.plans.logical.LogicalPlan

abstract class SQLBuilderTest
  extends LoggingFunSuite with TestUtils with BeforeAndAfterAll {

  protected val context = new LocalContext

  override protected def beforeAll(): Unit = {
    context range 10 select ('id as 'a) registerAsTable "t0"
    context range 10 select ('id as 'b) registerAsTable "t1"
  }

  override protected def afterAll(): Unit = {
    context.catalog removeRelation "t0"
    context.catalog removeRelation "t1"
  }

  protected def checkSQL(e: Expression, expectedSQL: String): Unit = {
    val maybeSQL = e.sql

    if (maybeSQL.isEmpty) {
      fail(
        s"""Cannot convert the following expression to its SQL form:
           |
           |${e.prettyTree}
           |""".stripMargin
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
             |""".stripMargin
        )
    }
  }

  protected def checkSQL(plan: LogicalPlan, expectedSQL: String): Unit = {
    val builder = new SQLBuilder(plan, context)
    val maybeSQL = builder.build()

    if (maybeSQL.isEmpty) {
      fail(
        s"""Cannot convert the following logical query plan to SQL:
           |
           |${plan.prettyTree}
           |""".stripMargin
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
             |""".stripMargin
        )
    }

    checkDataFrame(context.q(actualSQL), new DataFrame(plan, context))
  }

  protected def checkSQL(df: DataFrame, expectedSQL: String): Unit = {
    checkSQL(df.queryExecution.analyzedPlan, expectedSQL)
  }
}
