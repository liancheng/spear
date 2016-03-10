package scraper

import scraper.local.LocalContext

class DataFrameSuite extends LoggingFunSuite {
  private implicit val context = new LocalContext(Test.defaultSettings)

  test("column") {
    val df = context range 10
    assert(df('id) === df.queryExecution.analyzedPlan.output.head)
  }
}
