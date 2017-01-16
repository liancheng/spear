package scraper.fastparser

import fastparse.core.Logger

import scraper.utils.Logging

trait LoggingParser extends Logging {
  protected implicit val parsingLogger = Logger(logTrace(_))
}
