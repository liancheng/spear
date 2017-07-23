package spear

import spear.config.Settings.Key

package object config {
  val QueryCompilerClass: Key[String] = Key("spear.query-compiler").string
}
