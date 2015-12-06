package scraper

import scala.util.Random

package object generators {
  def randomPartition(rng: Random, sum: Int, partitions: Int): Seq[Int] = {
    assert(partitions <= sum)
    if (partitions == 1) {
      Seq(sum)
    } else {
      // see http://blog.csdn.net/morewindows/article/details/8439393 TODO: find an english blog
      val sorted = 0 +: Seq.fill(partitions - 1)(rng.nextInt(sum)).sorted :+ sum
      (sorted.tail, sorted.init).zipped.map(_ - _)
    }
  }
}
