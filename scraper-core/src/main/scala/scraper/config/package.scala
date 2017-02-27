package scraper

import scraper.config.Settings.Key

package object config {
  val QueryExecutorClass: Key[String] = Key("scraper.query-executor").string
}
