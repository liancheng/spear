import org.apache.log4j.{Level, Logger}

import scraper.config.Settings
import scraper.expressions._
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.local.LocalContext
import scraper.repl.Main.%
import scraper.types._

implicit val context = new LocalContext(Settings.load("scraper.conf", "scraper-reference.conf"))

import context._

def setLogLevel(level: String) {
  Logger.getRootLogger.setLevel(Level.toLevel(level))
}
