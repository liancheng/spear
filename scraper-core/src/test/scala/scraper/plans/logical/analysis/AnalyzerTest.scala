package scraper.plans.logical.analysis

import org.scalatest.BeforeAndAfterAll

import scraper.{InMemoryCatalog, LoggingFunSuite, TestUtils}
import scraper.parsers.QueryExpressionParser.queryExpression
import scraper.plans.logical.LogicalPlan

abstract class AnalyzerTest extends LoggingFunSuite with TestUtils with BeforeAndAfterAll {
  protected val catalog = new InMemoryCatalog

  protected val analyze = new Analyzer(catalog)

  protected def checkAnalyzedPlan(sql: String, expected: LogicalPlan): Unit =
    checkAnalyzedPlan(queryExpression.parse(sql).get.value, expected)

  protected def checkAnalyzedPlan(unresolved: LogicalPlan, expected: LogicalPlan): Unit =
    checkPlan(analyze(unresolved), expected)

  protected def checkMessage[T <: Throwable: Manifest](patterns: String*)(f: => Any): Unit = {
    val cause = intercept[T](f)

    (patterns foldLeft cause.getMessage) { (text, pattern) =>
      val index = text.indexOf(pattern)

      if (index == -1) {
        fail(
          s"""Failed to find the following pattern in the error message:
             |
             |  $pattern
             |
             |Full error message:
             |
             |  ${cause.getMessage}
           """.stripMargin
        )
      }

      text.drop(index + pattern.length)
    }
  }

  protected def checkMessageRegex[T <: Throwable: Manifest](patterns: String*)(f: => Any): Unit = {
    val regex = (patterns mkString ".*").r
    val cause = intercept[T](f)

    if (regex.findFirstIn(cause.getMessage).isEmpty) {
      fail(
        s"""Failed to find all of the following regex patterns in the error message:
           |
           |${patterns map ("  - " + _) mkString "\n"}
           |
           |Full error message:
           |
           |  ${cause.getMessage}
           |""".stripMargin
      )
    }
  }
}
