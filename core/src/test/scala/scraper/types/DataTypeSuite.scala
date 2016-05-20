package scraper.types

import scala.language.implicitConversions

import org.scalacheck.util.Pretty
import org.scalatest.prop.Checkers

import scraper.{LoggingFunSuite, TestUtils}
import scraper.expressions.{Attribute, ExpressionID}
import scraper.expressions.dsl._
import scraper.generators.types._

class DataTypeSuite extends LoggingFunSuite with TestUtils with Checkers {
  // ScalaCheck pretty printing support for `DataType`
  private implicit def prettyDataType(dataType: DataType): Pretty = Pretty {
    _ => "\n" + dataType.prettyTree
  }

  test("size of generated DataType") {
    check { t: PrimitiveType =>
      t.size == 1
    }

    check { t: ArrayType =>
      t.size == 1 + t.elementType.size
    }

    check { t: MapType =>
      t.size == 1 + t.keyType.size + t.valueType.size
    }
  }

  test("depth of generated DataType") {
    check { t: PrimitiveType =>
      t.depth == 1
    }

    check { t: ArrayType =>
      t.depth == 1 + t.elementType.depth
    }

    check { t: MapType =>
      t.depth == 1 + (t.keyType.depth max t.valueType.depth)
    }
  }

  test("ArrayType instantiation") {
    checkTree(
      ArrayType(IntType, elementNullable = true),
      ArrayType(IntType)
    )

    checkTree(
      ArrayType(IntType, elementNullable = false),
      ArrayType(IntType.!)
    )
  }

  test("MapType instantiation") {
    checkTree(
      MapType(IntType, StringType, valueNullable = true),
      MapType(IntType, StringType)
    )

    checkTree(
      MapType(IntType, StringType, valueNullable = false),
      MapType(IntType, StringType.!)
    )
  }

  test("StructType instantiation") {
    checkTree(
      StructType(StructField("f1", IntType, nullable = false) :: Nil),
      StructType('f1 -> IntType.!)
    )

    checkTree(
      StructType(Seq(
        StructField("f1", IntType, nullable = true),
        StructField("f2", DoubleType, nullable = false)
      )),
      StructType(
        'f1 -> IntType,
        'f2 -> DoubleType.!
      )
    )
  }

  test("StructType.rename") {
    checkTree(
      StructType(
        'f1 -> IntType.!,
        'f2 -> StringType.?
      ) rename ("c1", "c2"),
      StructType(
        'c1 -> IntType.!,
        'c2 -> StringType.?
      )
    )
  }

  test("StructType.toAttribute") {
    def resetID(attribute: Attribute): Attribute = attribute withID ExpressionID(0L)

    assertResult(Seq('f1.int.!, 'f2.string.?) map resetID) {
      StructType(
        "f1" -> IntType.!,
        "f2" -> StringType.?
      ).toAttributes map resetID
    }
  }

  private val testSchema =
    StructType(
      'name -> StringType.!,
      'age -> IntType,
      'gender -> StringType,
      'location -> StructType(
        'latitude -> DoubleType.!,
        'longitude -> DoubleType.!
      ),
      "phone-numbers" -> ArrayType(StringType.!),
      'addresses -> MapType(StringType, StringType.!)
    )

  test("StructType field types") {
    assertResult(IntType :: DoubleType :: Nil) {
      StructType(
        'f0 -> IntType.!,
        'f1 -> DoubleType
      ).fieldTypes
    }
  }

  test("pretty tree string of a StructType") {
    assertSideBySide(
      """struct
        |├╴name: string
        |├╴age: int?
        |├╴gender: string?
        |├╴location: struct?
        |│ ├╴latitude: double
        |│ ╰╴longitude: double
        |├╴phone-numbers: array?
        |│ ╰╴element: string
        |╰╴addresses: map?
        |  ├╴key: string
        |  ╰╴value: string
        |""".stripMargin.trim,
      testSchema.prettyTree
    )
  }

  test("orderable ArrayType") {
    val arrayType = ArrayType(IntType)
    assert(arrayType subtypeOf OrderedType)
  }

  test("non-orderable ArrayType") {
    val arrayType = ArrayType(MapType(IntType, StringType))
    assert(!(arrayType subtypeOf OrderedType))
  }

  test("orderable StructType") {
    val schema = StructType(
      'f0 -> IntType.!,
      'f1 -> DoubleType.!
    )

    assert(schema subtypeOf OrderedType)
  }

  test("non-orderable StructType") {
    val schema = StructType(
      'f0 -> IntType.!,
      'f1 -> MapType(IntType, StringType)
    )

    assert(!(schema subtypeOf OrderedType))
  }
}
