import scraper.config.Settings
import scraper.expressions.dsl._
import scraper.local.LocalContext

object BasicExample {
  def main(args: Array[String]) {
    val context = new LocalContext(Settings.load())
    val df0 = context range 10
    val df1 = df0 select ('id as 'a, 'id as 'b) where 'a > 3

    df1.explain()
    df1.show()
  }
}
