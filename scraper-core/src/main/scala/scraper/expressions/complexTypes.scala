package scraper.expressions

import scraper.{Name, Row}
import scraper.expressions.typecheck.TypeConstraint
import scraper.types._

case class CreateNamedStruct(names: Seq[Expression], values: Seq[Expression]) extends Expression {
  assert(names.length == values.length)

  override def isNullable: Boolean = false

  override def children: Seq[Expression] = names ++ values

  override def nodeName: Name = "named_struct"

  override def evaluate(input: Row): Any = Row.fromSeq(values map (_ evaluate input))

  override protected lazy val typeConstraint: TypeConstraint =
    (names sameTypeAs StringType andThen (_.foldable)) ++ values.pass

  override protected lazy val strictDataType: DataType = {
    val fields = (evaluatedNames, values map (_.dataType), values map (_.isNullable)).zipped map {
      (name, dataType, nullable) => StructField(Name.caseInsensitive(name), dataType, nullable)
    }

    StructType(fields)
  }

  override protected def template(childList: Seq[String]): String = {
    val (nameStrings, valueStrings) = childList splitAt names.length
    val argStrings = nameStrings zip valueStrings flatMap { case (name, value) => Seq(name, value) }
    argStrings mkString (s"$nodeName(", ", ", ")")
  }

  private lazy val evaluatedNames: Seq[String] =
    names map (_.evaluated match { case n: String => n })
}

case class CreateArray(values: Seq[Expression]) extends Expression {
  assert(values.nonEmpty)

  override def isNullable: Boolean = false

  override def children: Seq[Expression] = values

  override def nodeName: Name = "array"

  override def evaluate(input: Row): Any = values map (_ evaluate input)

  override protected lazy val typeConstraint: TypeConstraint = values.sameType

  override protected lazy val strictDataType: DataType =
    ArrayType(values.head.dataType, values exists (_.isNullable))
}

case class CreateMap(keys: Seq[Expression], values: Seq[Expression]) extends Expression {
  assert(keys.length == values.length)

  override def nodeName: Name = "map"

  override def isNullable: Boolean = false

  override def children: Seq[Expression] = keys ++ values

  override def evaluate(input: Row): Any =
    (keys map (_ evaluate input) zip (values map (_ evaluate input))).toMap

  override protected lazy val typeConstraint: TypeConstraint = keys.sameType ++ values.sameType

  override protected lazy val strictDataType: DataType = {
    val valueNullable = values exists (_.isNullable)
    MapType(keys.head.dataType, values.head.dataType, valueNullable)
  }
}
