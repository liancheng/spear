import scraper.Context._
import scraper.config.Settings
import scraper.expressions._
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.local.LocalContext
import scraper.types._
import scraper.utils.loadConfig

implicit val context = new LocalContext(new Settings(loadConfig("full")))

context range 100 select (
  If(
    ((rand(42) * 10) cast IntType) % 10 < 2,
    lit(null),
    'id
  ) as 'id
) asTable 't
