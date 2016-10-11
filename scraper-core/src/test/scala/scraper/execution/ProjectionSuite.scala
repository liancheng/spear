package scraper.execution

import scala.collection.mutable.ArrayBuffer

import scraper.{BasicMutableRow, LoggingFunSuite, Row}
import scraper.expressions._

class ProjectionSuite extends LoggingFunSuite {
  test("projection") {
    val projection = Projection(Seq(1, "foo"))
    assert(projection() == Row(1, "foo"))
  }

  test("mutable projection") {
    val projection = MutableProjection(Seq('_.int at 0, '_.string at 1))
    assert(projection(Row(1, "foo")) == Row(1, "foo"))
    assert(projection(Row(2, "bar")) == Row(2, "bar"))
  }

  test("mutable projection - target change") {
    val row = new BasicMutableRow(ArrayBuffer[Any](1, "foo"))
    val projection = MutableProjection(Seq('_.int at 0, '_.string at 1)) target row
    assert(projection(Row(2, "bar")) == Row(2, "bar"))
  }
}
