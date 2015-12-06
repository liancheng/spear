package scraper.expressions

import org.scalacheck.{Gen, Prop, Test}
import org.scalatest.prop.Checkers

import scraper.expressions.functions._
import scraper.generators.types._
import scraper.generators.values._
import scraper.plans.logical.LocalRelation
import scraper.types.{TestUtils, TupleType}
import scraper.{DataFrame, LocalContext, LoggingFunSuite, Row}

class AggregationSuite extends LoggingFunSuite with Checkers with TestUtils {
  private val context = new LocalContext

  private val genNumericValues = for {
    t <- genNumericType
    values <- Gen listOfN (10, genValueForNumericType(t))
  } yield (t, values)

  private def testUnaryArithmeticAggregation(
    aggregate: AggregateExpression,
    verify: (Seq[Any], Numeric[Any]) => Any
  ): Unit = {
    test(s"aggregation - ${aggregate.getClass.getSimpleName stripSuffix "$"}") {
      val prop = Prop.forAll(genNumericValues) {
        case (t, values) =>
          val rows = values map (Row(_))
          val df = new DataFrame(LocalRelation(rows, TupleType("n" -> t.!)), context)
          val numeric = t.genericNumeric
          val ordering = t.genericOrdering

          checkDataFrame(
            df agg (aggregate as 'a),
            Row(verify(values, numeric))
          )

          checkDataFrame(
            df groupBy 'n agg (aggregate as 'a) orderBy 'a,
            values
              .groupBy(identity)
              .mapValues(verify(_, numeric))
              .values
              .toSeq
              .sorted(ordering)
              .map(Row(_))
          )
      }

      check(prop, Test.Parameters.defaultVerbose.withMinSize(10))
    }
  }

  testUnaryArithmeticAggregation(sum('n), _ sum _)
  testUnaryArithmeticAggregation(min('n), _ min _)
  testUnaryArithmeticAggregation(max('n), _ max _)
}
