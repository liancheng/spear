package spear

import scala.util.{Failure, Success}

import org.scalacheck.Gen

import spear.config.Settings.Key
import spear.exceptions.SettingsValidationException

package object generators {
  def genRandomPartitions(sum: Int, partitionNum: Int): Gen[Seq[Int]] = for {
    ns <- Gen pick (partitionNum - 1, 1 until sum)
    sorted = ns.sorted
  } yield (0 +: sorted, sorted :+ sum).zipped.map(_ - _)

  def chance[T](gs: (Double, Gen[T])*): Gen[T] = {
    val (chances, gens) = gs.unzip
    val frequencies = chances map { _ * 100 } map { _.toInt }
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
      Key("spear.test.types.allow-null-type").boolean

    val AllowEmptyStructType: Key[Boolean] =
      Key("spear.test.types.allow-empty-struct-type").boolean

    val AllowNullableComplexType: Key[Boolean] =
      Key("spear.test.types.allow-nullable-complex-type").boolean

    val AllowNullableArrayType: Key[Boolean] =
      Key("spear.test.types.allow-nullable-array-type").boolean

    val AllowNullableMapType: Key[Boolean] =
      Key("spear.test.types.allow-nullable-map-type").boolean

    val AllowNullableStructField: Key[Boolean] =
      Key("spear.test.types.allow-nullable-struct-field").boolean

    val AllowNestedStructType: Key[Boolean] =
      Key("spear.test.types.allow-nested-struct-type").boolean

    val MaxStructTypeWidth: Key[Int] =
      Key("spear.test.types.max-struct-type-width").int

    // ----------------------------------
    // Settings keys for value generators
    // ----------------------------------

    val MaxRepetition: Key[Int] =
      Key("spear.test.expressions.max-repetition").int

    val NullChances: Key[Double] =
      Key("spear.test.expressions.chances.null").double.validate {
        case v if v >= 0D && v <= 1.0D => Success(v)
        case v => Failure(new SettingsValidationException(
          s"Illegal null chance $v, value must be within range [0.0, 1.0]."
        ))
      }

    val OnlyLogicalOperatorsInPredicate: Key[Boolean] =
      Key("spear.test.expressions.only-logical-operators-in-predicate").boolean

    // -----------------------------------------
    // Settings keys for logical plan generators
    // -----------------------------------------

    val SelectClauseChance: Key[Double] =
      Key("spear.test.plans.chances.select-clause").double

    val FromClauseChance: Key[Double] =
      Key("spear.test.plans.chances.from-clause").double

    val WhereClauseChance: Key[Double] =
      Key("spear.test.plans.chances.where-clause").double

    val LimitClauseChance: Key[Double] =
      Key("spear.test.plans.chances.limit-clause").double

    val MaxJoinNum: Key[Int] =
      Key("spear.test.plans.max-join-num").int

    val MaxProjectWidth: Key[Int] =
      Key("spear.test.plans.max-project-width").int

    val MaxLimit: Key[Int] =
      Key("spear.test.plans.max-limit").int

    val MaxExpressionSize: Key[Int] =
      Key("spear.test.plans.max-expression-size").int

    val MaxWherePredicateSize: Key[Int] =
      Key("spear.test.plans.max-where-predicate-size").int

    val MaxSelectExpressionSize: Key[Int] =
      Key("spear.test.plans.max-select-expression-size").int
  }
}
