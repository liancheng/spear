package scraper.plans.logical.analysis

import org.scalatest.BeforeAndAfterAll

import scraper.{InMemoryCatalog, LoggingFunSuite, TestUtils}
import scraper.parser.Parser
import scraper.plans.logical.LogicalPlan

abstract class AnalyzerTest extends LoggingFunSuite with TestUtils with BeforeAndAfterAll {
  protected val catalog = new InMemoryCatalog

  protected val analyze = new Analyzer(catalog)

  protected def checkAnalyzedPlan(sql: String, expected: LogicalPlan): Unit =
    checkAnalyzedPlan(new Parser parse sql, expected)

  protected def checkAnalyzedPlan(unresolved: LogicalPlan, expected: LogicalPlan): Unit =
    checkPlan(analyze(unresolved), expected)
}
