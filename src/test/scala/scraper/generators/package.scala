package scraper

import scala.util.Random

import org.scalacheck.Gen

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

  def chance[T](gs: (Double, Gen[T])*): Gen[T] = {
    val (chances, gens) = gs.unzip
    val frequencies = chances map (_ * 100) map (_.toInt)
    Gen.frequency(frequencies zip gens: _*)
  }

  def chanceOption[T](chance: Double, gen: Gen[T]): Gen[Option[T]] = Gen.sized {
    case 0 => Gen.const(None)
    case _ => generators.chance(chance -> gen.map(Some.apply), (1 - chance) -> Gen.const(None))
  }
}
