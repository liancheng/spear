import scraper.Context
import scraper.config.Settings
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.local.LocalContext

object BasicExample {
  def main(args: Array[String]) {
    val context = new LocalContext(Settings.load())

    simpleQuery(context)
    aggregateWithHavingAndOrderBy(context)
  }

  def simpleQuery(context: Context): Unit = {
    val df = context range 10 select ('id as 'a, 'id as 'b) where 'a > 3
    df.explain()
    df.show()
  }

  def aggregateWithHavingAndOrderBy(context: Context): Unit = {
    val df = context
      .range(10)
      .select('id as 'a, 'id as 'b, 'id as 'c)
      .groupBy('a)
      .agg(count('b))
      .having('a > 3 && max('c) < 4)
      .orderBy(min('b))

    df.explain()
    df.show()
  }
}
