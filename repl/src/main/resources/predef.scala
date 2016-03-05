import org.apache.log4j.{Level, Logger}

import scraper.config.Settings
import scraper.local.LocalContext
import scraper.utils.loadConfig

import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.types._

val context = new LocalContext(new Settings(loadConfig("scraper")))

def setLogLevel(level: String) {
  Logger.getRootLogger.setLevel(Level.toLevel(level))
}
