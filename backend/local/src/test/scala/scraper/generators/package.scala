package scraper

import org.scalacheck.Gen

import scraper.config.Settings.Key

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

  object Keys {
    // ---------------------------------
    // Settings keys for type generators
    // ---------------------------------

    val AllowNullType: Key[Boolean] =
      Key("scraper.test.types.allow-null-type").boolean

    val AllowEmptyStructType: Key[Boolean] =
      Key("scraper.test.types.allow-empty-struct-type").boolean

    val AllowNullableComplexType: Key[Boolean] =
      Key("scraper.test.types.allow-nullable-complex-type").boolean

    val AllowNullableArrayType: Key[Boolean] =
      Key("scraper.test.types.allow-nullable-array-type").boolean

    val AllowNullableMapType: Key[Boolean] =
      Key("scraper.test.types.allow-nullable-map-type").boolean

    val AllowNullableStructField: Key[Boolean] =
      Key("scraper.test.types.allow-nullable-struct-field").boolean

    val AllowNestedStructType: Key[Boolean] =
      Key("scraper.test.types.allow-nested-struct-type").boolean

    val MaxStructTypeWidth: Key[Int] =
      Key("scraper.test.types.max-struct-type-width").int

    // ----------------------------------
    // Settings keys for value generators
    // ----------------------------------

    val MaxRepetition: Key[Int] =
      Key("scraper.test.expressions.max-repetition").int

    // -----------------------------------------
    // Settings keys for logical plan generators
    // -----------------------------------------

    val SelectClauseChance: Key[Double] =
      Key("scraper.test.plans.chances.select-clause").double

    val FromClauseChance: Key[Double] =
      Key("scraper.test.plans.chances.from-clause").double

    val WhereClauseChance: Key[Double] =
      Key("scraper.test.plans.chances.where-clause").double

    val LimitClauseChance: Key[Double] =
      Key("scraper.test.plans.chances.limit-clause").double

    val MaxJoinNum: Key[Int] =
      Key("scraper.test.plans.max-join-num").int

    val MaxProjectWidth: Key[Int] =
      Key("scraper.test.plans.max-project-width").int

    val MaxLimit: Key[Int] =
      Key("scraper.test.plans.max-limit").int

    val MaxExpressionSize: Key[Int] =
      Key("scraper.test.plans.max-expression-size").int

    val MaxWherePredicateSize: Key[Int] =
      Key("scraper.test.plans.max-where-predicate-size").int

    val MaxSelectExpressionSize: Key[Int] =
      Key("scraper.test.plans.max-select-expression-size").int
  }
}
