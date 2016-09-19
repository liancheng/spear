package scraper.expressions.windows

import scala.util.Try

import scraper.LoggingFunSuite
import scraper.expressions._
import scraper.types.{DoubleType, IntType, StringType}

class WindowSpecSuite extends LoggingFunSuite {
  test("rows window frame") {
    assertResult("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING") {
      WindowFrame(RowsFrame, UnboundedPreceding, UnboundedFollowing).toString
    }

    assertResult("ROWS BETWEEN 10 PRECEDING AND CURRENT ROW") {
      WindowFrame(RowsFrame, Preceding(10), CurrentRow).toString
    }

    assertResult("ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING") {
      WindowFrame(RowsFrame, CurrentRow, Following(10)).toString
    }

    assertResult("ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING") {
      WindowFrame(RowsFrame, Preceding(2), Preceding(1)).toString
    }

    assertResult("ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING") {
      WindowFrame(RowsFrame, Following(1), Following(2)).toString
    }
  }

  test("range window frame") {
    assertResult("RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING") {
      WindowFrame(RangeFrame, UnboundedPreceding, UnboundedFollowing).toString
    }

    assertResult("RANGE BETWEEN 10 PRECEDING AND CURRENT ROW") {
      WindowFrame(RangeFrame, Preceding(10), CurrentRow).toString
    }

    assertResult("RANGE BETWEEN CURRENT ROW AND 10 FOLLOWING") {
      WindowFrame(RangeFrame, CurrentRow, Following(10)).toString
    }

    assertResult("RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING") {
      WindowFrame(RangeFrame, Preceding(2), Preceding(1)).toString
    }

    assertResult("RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING") {
      WindowFrame(RangeFrame, Following(1), Following(2)).toString
    }
  }

  test("invalid frame boundary") {
    intercept[IllegalArgumentException](Preceding(-1))
    intercept[IllegalArgumentException](Following(-1))
  }

  test("invalid window frame") {
    intercept[IllegalArgumentException](WindowFrame(RowsFrame, Following(1), CurrentRow))
    intercept[IllegalArgumentException](WindowFrame(RowsFrame, CurrentRow, Preceding(1)))
  }

  test("window spec") {
    val rowsFrame = WindowFrame.Default.copy(frameType = RowsFrame)
    val rangeFrame = WindowFrame.Default.copy(frameType = RangeFrame)

    def checkWindowSpec(sql: String)(spec: => WindowSpec): Unit = assertResult(Try(sql))(spec.sql)

    Seq(rowsFrame, rangeFrame) foreach { frame =>
      checkWindowSpec(s"(PARTITION BY a, b ORDER BY c ASC NULLS FIRST $frame)") {
        Window partitionBy (a, b) orderBy c.asc.nullsFirst between frame
      }

      checkWindowSpec(s"(PARTITION BY a, b ORDER BY c ASC NULLS FIRST $frame)") {
        Window orderBy c.asc.nullsFirst partitionBy (a, b) between frame
      }

      checkWindowSpec(s"(ORDER BY c ASC NULLS FIRST $frame)") {
        Window orderBy c.asc.nullsFirst between frame
      }

      checkWindowSpec(s"(PARTITION BY a, b $frame)") {
        Window partitionBy (a, b) between frame
      }

      checkWindowSpec(s"($frame)") {
        Window between frame
      }
    }

    checkWindowSpec(s"($rowsFrame)") {
      Window rowsBetween (UnboundedPreceding, UnboundedFollowing)
    }

    checkWindowSpec(s"($rangeFrame)") {
      Window rangeBetween (UnboundedPreceding, UnboundedFollowing)
    }
  }

  private val (a, b, c) = ('a of IntType, 'b of StringType, 'c of DoubleType)
}
