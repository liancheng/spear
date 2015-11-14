package scraper

import scala.language.implicitConversions

package object types {
  implicit def `(String,Schema)->TupleField`(pair: (String, Schema)): TupleField = {
    val (name, Schema(dataType, nullable)) = pair
    TupleField(name, dataType, nullable)
  }

  implicit def `(Symbol,Schema)->TupleField`(pair: (Symbol, Schema)): TupleField = {
    val (name, Schema(dataType, nullable)) = pair
    TupleField(name.name, dataType, nullable)
  }
}
