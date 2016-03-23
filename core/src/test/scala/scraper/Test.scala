package scraper

import scraper.config.Settings

object Test {
  implicit val defaultSettings: Settings = Settings.load("scraper-test.conf")
}
