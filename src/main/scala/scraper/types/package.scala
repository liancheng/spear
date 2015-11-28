package scraper

import scala.language.implicitConversions

package object types {
  implicit def `(String,FieldSpec)->TupleField`(pair: (String, FieldSpec)): TupleField = {
    val (name, FieldSpec(dataType, nullable)) = pair
    TupleField(name, dataType, nullable)
  }

  implicit def `(Symbol,FieldSpec)->TupleField`(pair: (Symbol, FieldSpec)): TupleField = {
    val (name, FieldSpec(dataType, nullable)) = pair
    TupleField(name.name, dataType, nullable)
  }
}
