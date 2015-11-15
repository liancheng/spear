package scraper

import scraper.expressions.Literal
import scraper.expressions.dsl._
import scraper.types.TestUtils
import scraper.utils.sideBySide

class LocalContextSuite extends LoggingFunSuite with TestUtils {
  private val context = new LocalContext

  def checkDataset(ds: Dataset, expected: Row): Unit = checkDataset(ds, expected :: Nil)

  def checkDataset(ds: Dataset, expected: => Seq[Row]): Unit = {
    val actual = ds.queryExecution.physicalPlan.iterator.toSeq
    if (actual != expected) {
      val explanation = ds.explanation(extended = true)

      val answerDiff = sideBySide(
        s"""Expected answer:
           |${expected mkString "\n"}
         """.stripMargin,

        s"""Actual answer:
           |${actual mkString "\n"}
         """.stripMargin,

        withHeader = true
      )

      fail(
        s"""Dataset contains unexpected rows:
           |
           |$answerDiff
           |Query plan details:
           |
           |$explanation
         """.stripMargin
      )
    }
  }

  test("foo") {
    val data = Seq(1 -> "a", 2 -> "b")
    val ds = context lift data select ('_2, '_1) filter ('_1 <> (1 + Literal(1)))
    checkDataset(ds, Row("a", 1))
  }

  test("bar") {
    checkDataset(context select (1 as 'a) select 'a, Row(1))
  }
}
