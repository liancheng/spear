package scraper

import scala.util.Random

package object generators {
  def randomPartition(rng: Random, sum: Int, partitions: Int): Seq[Int] = {
    if (partitions == 1) {
      Seq(sum)
    } else {
      val sorted = Seq.fill(partitions) { rng.nextInt(sum) }.sorted
      sorted.head +: (sorted.tail, sorted.init).zipped.map(_ - _)
    }
  }
}
