package scraper.config

import scraper.config.Settings.Key

object Keys {
  val NullsLarger: Key[Boolean] =
    Key("scraper.language.nulls-larger").boolean

  val CaseSensitive: Key[Boolean] =
    Key("scraper.language.case-sensitive").boolean
}
