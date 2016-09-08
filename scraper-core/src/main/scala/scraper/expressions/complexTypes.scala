package scraper.expressions

import scraper.{Name, Row}
import scraper.expressions.typecheck.{Foldable, StrictlyTyped, TypeConstraint}
import scraper.types._

case class CreateNamedStruct(children: Seq[Expression]) extends Expression {
  assert(children.length % 2 == 0)

  override def isNullable: Boolean = false

  override def nodeName: Name = "named_struct"

  override def evaluate(input: Row): Any = Row.fromSeq(values map (_ evaluate input))

  override protected def typeConstraint: TypeConstraint =
    (names sameTypeAs StringType andAlso Foldable) ++ StrictlyTyped(values)

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

  private lazy val (names, values) = children.splitAt(children.length / 2)

  private lazy val evaluatedNames: Seq[String] =
    names map (_.evaluated match { case n: String => n })
}

object CreateNamedStruct {
  def apply(names: Seq[Expression], values: Seq[Expression]): CreateNamedStruct =
    CreateNamedStruct(names ++ values)
}

case class CreateArray(values: Seq[Expression]) extends Expression {
  assert(values.nonEmpty)

  override def isNullable: Boolean = false

  override def children: Seq[Expression] = values

  override def nodeName: Name = "array"

  override def evaluate(input: Row): Any = values map (_ evaluate input)

  override protected def typeConstraint: TypeConstraint = values.sameType

  override protected lazy val strictDataType: DataType =
    ArrayType(values.head.dataType, values exists (_.isNullable))
}

case class CreateMap(children: Seq[Expression]) extends Expression {
  assert(children.length % 2 == 0)

  override def nodeName: Name = "map"

  override def isNullable: Boolean = false

  override def evaluate(input: Row): Any =
    (keys map (_ evaluate input) zip (values map (_ evaluate input))).toMap

  override protected def typeConstraint: TypeConstraint = keys.sameType ++ values.sameType

  override protected lazy val strictDataType: DataType = {
    val valueNullable = values exists (_.isNullable)
    MapType(keys.head.dataType, values.head.dataType, valueNullable)
  }

  private lazy val (keys, values) = children.splitAt(children.length / 2)
}

object CreateMap {
  def apply(keys: Seq[Expression], values: Seq[Expression]): CreateMap = CreateMap(keys ++ values)
}
