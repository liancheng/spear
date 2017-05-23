package spear.parsers

import fastparse.core.Logger

import spear.utils.Logging

trait LoggingParser extends Logging {
  protected implicit val parsingLogger = Logger(logTrace(_))
}
