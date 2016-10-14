package scraper

import scraper.config.Settings.Key

package object config {
  val QueryExecutor = Key("scraper.query-executor").string
}
