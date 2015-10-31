package scraper

import scraper.types.DataType

package object generators {
  /**
   * A class that constraints max depth and size of a generated data type.
   *
   * @param maxDepth The max depth of the generated [[DataType]]
   * @param maxSize The max size of the generated [[DataType]]
   */
  case class DataTypeDim(maxDepth: Int, maxSize: Int)

  implicit val defaultDataTypeDim: DataTypeDim = DataTypeDim(maxDepth = 8, maxSize = 32)

  /**
   * A class that constraints max size of a generated value of a data type.
   *
   * @param maxRepetition The max number of element of a generated map or array.
   */
  case class ValueDim(maxRepetition: Int) {
    assert(maxRepetition >= 0, s"Max repetition must be non-negative: $maxRepetition")
  }

  implicit val defaultValueDim: ValueDim = ValueDim(maxRepetition = 8)
}
