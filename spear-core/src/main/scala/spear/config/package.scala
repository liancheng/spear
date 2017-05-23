package spear

import spear.config.Settings.Key

package object config {
  val QueryExecutorClass: Key[String] = Key("spear.query-executor").string
}
