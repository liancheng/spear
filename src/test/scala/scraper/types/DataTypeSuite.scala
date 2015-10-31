package scraper.types

import scala.language.implicitConversions

import org.scalacheck.Prop.forAll
import org.scalacheck.util.Pretty
import org.scalatest.prop.Checkers
import scraper.generators._
import scraper.generators.types._

class DataTypeSuite extends TestUtils with Checkers {
  // ScalaCheck pretty printing support for `DataType`
  private implicit def prettyDataType(dataType: DataType): Pretty = Pretty {
    _ => "\n" + dataType.prettyTree
  }

  test("size of generated DataType") {
    check(forAll { dataType: DataType =>
      dataType.size <= defaultDataTypeDim.maxSize
    })
  }

  test("depth of generated DataType") {
    check(forAll { dataType: DataType =>
      dataType.depth <= defaultDataTypeDim.maxDepth
    })
  }

  test("ArrayType instantiation") {
    assertSideBySide(
      ArrayType(IntType, elementNullable = true),
      ArrayType(IntType.?)
    )

    assertSideBySide(
      ArrayType(IntType, elementNullable = false),
      ArrayType(IntType.!)
    )
  }

  test("MapType instantiation") {
    assertSideBySide(
      MapType(IntType, StringType, valueNullable = true),
      MapType(IntType, StringType.?)
    )

    assertSideBySide(
      MapType(IntType, StringType, valueNullable = false),
      MapType(IntType, StringType.!)
    )
  }

  test("StructType instantiation") {
    assertSideBySide(
      StructType(StructField("f1", IntType, nullable = false) :: Nil),
      StructType('f1 -> IntType.!)
    )

    assertSideBySide(
      StructType(Seq(
        StructField("f1", IntType, nullable = true),
        StructField("f2", DoubleType, nullable = false)
      )),
      StructType(
        'f1 -> IntType.?,
        'f2 -> DoubleType.!
      )
    )
  }

  private val testSchema =
    StructType(
      'name -> StringType.!,
      'age -> IntType.?,
      'gender -> StringType.?,
      'location -> StructType(
        'latitude -> DoubleType.!,
        'longitude -> DoubleType.!
      ).?,
      "phone-numbers" -> ArrayType(StringType.!).?,
      'addresses -> MapType(StringType, StringType.!).?
    )

  test("StructType field types") {
    assertResult(IntType :: DoubleType :: Nil) {
      StructType(
        'f0 -> IntType.!,
        'f1 -> DoubleType.?
      ).fieldTypes
    }
  }

  test("pretty tree string of a StructType") {
    assertSideBySide(
      testSchema.prettyTree,
      """struct
        | ├─ name: string
        | ├─ age: int?
        | ├─ gender: string?
        | ├─ location: struct?
        | │   ├─ latitude: double
        | │   └─ longitude: double
        | ├─ phone-numbers: array?
        | │   └─ element: string
        | └─ addresses: map?
        |     ├─ key: string
        |     └─ value: string
      """.stripMargin.trim
    )
  }
}
