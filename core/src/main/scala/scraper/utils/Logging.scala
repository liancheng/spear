package scraper.utils

import org.slf4j.LoggerFactory

trait Logging {
  protected val logger = LoggerFactory.getLogger(getClass)

  protected def logTrace(message: => String): Unit = {
    if (logger.isTraceEnabled) {
      logger.trace(message)
    }
  }

  protected def logDebug(message: => String): Unit = {
    if (logger.isDebugEnabled) {
      logger.debug(message)
    }
  }

  protected def logInfo(message: => String): Unit = {
    if (logger.isInfoEnabled) {
      logger.info(message)
    }
  }

  protected def logWarn(message: => String): Unit = {
    if (logger.isWarnEnabled) {
      logger.warn(message)
    }
  }

  protected def logError(message: => String): Unit = {
    if (logger.isErrorEnabled) {
      logger.error(message)
    }
  }
}
