package scraper.execution

import scraper.{BasicMutableRow, MutableRow, Row}
import scraper.expressions.Expression

trait Projection extends (Row => Row) with (() => Row)

object Projection {
  def apply(expressions: Seq[Expression]): Projection = new Projection {
    override def apply(input: Row): Row = Row.fromSeq(expressions map (_ evaluate input))

    override def apply(): Row = apply(null)
  }
}

trait MutableProjection extends Projection {
  def target(mutableRow: MutableRow): this.type
}

object MutableProjection {
  def apply(expressions: Seq[Expression]): MutableProjection = new MutableProjection {
    private[this] var mutableRow: MutableRow = new BasicMutableRow(expressions.length)

    override def target(mutableRow: MutableRow): this.type = {
      this.mutableRow = mutableRow
      this
    }

    override def apply(input: Row): Row = {
      expressions.map(_ evaluate input).zipWithIndex foreach {
        case (value, ordinal) => mutableRow(ordinal) = value
      }

      mutableRow
    }

    override def apply(): Row = apply(null)
  }
}
