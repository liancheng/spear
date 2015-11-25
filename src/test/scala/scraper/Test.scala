package scraper

import scraper.config.Settings
import scraper.utils.loadConfig

object Test {
  implicit val defaultSettings: Settings = Settings(loadConfig("test"))
}
