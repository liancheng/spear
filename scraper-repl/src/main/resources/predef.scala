import org.apache.log4j.{Level, Logger}

import scraper.Context
import scraper.config.Settings
import scraper.expressions._
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.expressions.windows._
import scraper.Row
import scraper.repl.Main.%
import scraper.types._

implicit val context = new Context(Settings.load("scraper.conf", "scraper-reference.conf"))

import context._

def setLogLevel(level: String) {
  Logger.getRootLogger.setLevel(Level.toLevel(level))
}
