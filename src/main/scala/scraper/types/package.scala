package scraper

import scala.language.implicitConversions

package object types {
  implicit def `(String,FieldSpec)->StructField`(pair: (String, FieldSpec)): StructField = {
    val (name, FieldSpec(dataType, nullable)) = pair
    StructField(name, dataType, nullable)
  }

  implicit def `(Symbol,FieldSpec)->StructField`(pair: (Symbol, FieldSpec)): StructField = {
    val (name, FieldSpec(dataType, nullable)) = pair
    StructField(name.name, dataType, nullable)
  }
}
