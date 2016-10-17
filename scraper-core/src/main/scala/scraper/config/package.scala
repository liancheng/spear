package scraper

import scraper.config.Settings.Key

package object config {
  val QueryExecutorClass = Key("scraper.query-executor").string
}
