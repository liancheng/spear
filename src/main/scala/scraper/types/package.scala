package scraper

import scala.language.implicitConversions

package object types {
  implicit def `(String,Schema)->StructField`(pair: (String, Schema)): StructField = {
    val (name, Schema(dataType, nullable)) = pair
    StructField(name, dataType, nullable)
  }

  implicit def `(Symbol,Schema)->StructField`(pair: (Symbol, Schema)): StructField = {
    val (name, Schema(dataType, nullable)) = pair
    StructField(name.name, dataType, nullable)
  }
}
