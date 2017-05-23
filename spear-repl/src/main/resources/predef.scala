import org.apache.log4j.{Level, Logger}

import spear.Context
import spear.config.Settings
import spear.expressions._
import spear.expressions.dsl._
import spear.expressions.functions._
import spear.expressions.windows._
import spear.Row
import spear.repl.Main.%
import spear.types._

implicit val context = new Context(Settings.load("spear.conf", "spear-reference.conf"))

import context._

def setLogLevel(level: String) {
  Logger.getRootLogger.setLevel(Level.toLevel(level))
}
