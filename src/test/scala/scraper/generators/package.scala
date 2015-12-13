package scraper

import org.scalacheck.Gen

package object generators {
  def genRandomPartitions(sum: Int, partitionNum: Int): Gen[Seq[Int]] = for {
    ns <- Gen pick (partitionNum - 1, 1 until sum)
    sorted = ns.sorted
  } yield (0 +: sorted, sorted :+ sum).zipped.map(_ - _)

  def chance[T](gs: (Double, Gen[T])*): Gen[T] = {
    val (chances, gens) = gs.unzip
    val frequencies = chances map (_ * 100) map (_.toInt)
    Gen.frequency(frequencies zip gens: _*)
  }

  def chanceOption[T](chance: Double, gen: Gen[T]): Gen[Option[T]] = Gen.sized {
    case 0 => Gen const None
    case _ => generators.chance(chance -> gen.map(Some.apply), (1 - chance) -> Gen.const(None))
  }
}
