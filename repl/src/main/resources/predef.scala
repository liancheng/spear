import scraper.config.Settings
import scraper.local.LocalContext
import scraper.utils.loadConfig

val context = new LocalContext(new Settings(loadConfig("scraper")))

import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.types._
