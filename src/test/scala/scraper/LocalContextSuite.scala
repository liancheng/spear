package scraper

import scraper.expressions.Literal
import scraper.expressions.dsl._
import scraper.types.TestUtils

class LocalContextSuite extends TestUtils {
  test("foo") {
    val data = Seq(1 -> "a", 2 -> "b")
    val ds = new LocalContext lift data select ('_1 as 'a) filter ('a <> (1 + Literal(1)))
    ds.explain(true)
    assert(ds.queryExecution.physicalPlan.iterator.toSeq === Seq(Row(1, "a")))
  }
}
