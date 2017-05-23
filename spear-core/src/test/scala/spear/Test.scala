package spear

import spear.config.Settings

object Test {
  implicit val defaultSettings: Settings = Settings.load("spear-test.conf")
}
