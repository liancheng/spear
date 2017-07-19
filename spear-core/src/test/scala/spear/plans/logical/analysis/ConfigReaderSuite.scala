package spear.plans.logical.analysis

import com.typesafe.config.ConfigFactory

import spear.LoggingFunSuite
import spear.plans.logical.analysis.ConfigReaderSuite.MultiPhaseTransformerConfig
import spear.trees.ConvergenceTest

class ConfigReaderSuite extends LoggingFunSuite {
  test("foo") {
    import pureconfig._

    implicit val coproductHint = new FieldCoproductHint[ConvergenceTest]("type") {
      override protected def fieldValue(name: String): String = {
        ConfigFieldMapping(CamelCase, CamelCase)(name)
      }
    }

    val config = ConfigFactory.parseString(
      """spear.analyzer {
        |  phases: [{
        |    name: "Pre-processing",
        |    convergence-test: { type: "Once" },
        |    rules: [
        |      "RewriteCTEsAsSubquery",
        |      "InlineWindowDefinitions"
        |    ]
        |  }, {
        |    name: "Resolution",
        |    convergence-test: { type: "FixedPoint" },
        |    rules: [
        |      "ResolveRelation",
        |      "InlineWindowDefinitions",
        |      "RewriteRenameToProject",
        |      "DeduplicateReferences",
        |
        |      "ExpandStar",
        |      "ResolveReference",
        |      "ResolveFunction",
        |      "ResolveAlias",
        |
        |      "ExtractWindowFunctions",
        |      "RewriteDistinctAggregateFunction",
        |      "RewriteDistinctToAggregate",
        |      "RewriteProjectToGlobalAggregate",
        |      "UnifyFilteredSortedAggregate",
        |      "RewriteUnresolvedAggregate"
        |    ]
        |  }]
        |}
        |""".stripMargin
    )

    loadConfigOrThrow[MultiPhaseTransformerConfig](config, namespace = "spear.analyzer")
  }
}

object ConfigReaderSuite {
  case class PhaseConfig(
    name: String,
    convergenceTest: ConvergenceTest,
    rules: Seq[String]
  )

  case class MultiPhaseTransformerConfig(phases: Seq[PhaseConfig])
}
