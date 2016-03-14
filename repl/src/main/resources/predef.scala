import org.apache.log4j.{Level, Logger}

import scraper.config.Settings
import scraper.local.LocalContext
import scraper.utils.loadConfig

import scraper.expressions._
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.types._

import scraper.Context._

implicit val context = new LocalContext(new Settings(loadConfig("full")))

def setLogLevel(level: String) {
  Logger.getRootLogger.setLevel(Level.toLevel(level))
}
