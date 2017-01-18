package scraper.parsers

import fastparse.core.Logger

import scraper.{LoggingFunSuite, TestUtils}
import scraper.types._

class DataTypeParserSuite extends LoggingFunSuite with TestUtils {
  import fastparse.all._

  testDataTypeParsing("BOOLEAN", BooleanType)

  testDataTypeParsing("TINYINT", ByteType)

  testDataTypeParsing("SMALLINT", ShortType)

  testDataTypeParsing("INT", IntType)

  testDataTypeParsing("BIGINT", LongType)

  testDataTypeParsing("FLOAT", FloatType)

  testDataTypeParsing("DOUBLE", DoubleType)

  testDataTypeParsing("ARRAY<INT>", ArrayType(IntType.?))

  testDataTypeParsing("MAP<INT, STRING>", MapType(IntType, StringType.?))

  testDataTypeParsing(
    "STRUCT<name: STRING, age: INT>",
    StructType(
      'name -> StringType.?,
      'age -> IntType.?
    )
  )

  testDataTypeParsing(
    "STRUCT<\"name\": STRING, \"age\": INT>",
    StructType(
      'name -> StringType.?,
      'age -> IntType.?
    )
  )

  testDataTypeParsing(
    "ROW(name STRING, age INT)",
    StructType(
      'NAME -> StringType.?,
      'AGE -> IntType.?
    )
  )

  testDataTypeParsing(
    "ROW(\"name\" STRING, \"age\" INT)",
    StructType(
      "name" -> StringType.?,
      "age" -> IntType.?
    )
  )

  private def testDataTypeParsing(input: String, expected: DataType): Unit = {
    test(s"parsing expression: $input") {
      checkTree(parse(input), expected)
    }
  }

  private def parse(input: String): DataType = {
    implicit val parserLogger = Logger(logInfo(_))
    (Start ~ DataTypeParser.dataType.log() ~ End parse input.trim).get.value
  }
}
