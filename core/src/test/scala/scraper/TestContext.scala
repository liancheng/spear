package scraper

import scraper.config.Settings
import scraper.expressions.Attribute
import scraper.parser.DeprecatedParser
import scraper.plans.QueryExecution
import scraper.plans.logical.{Analyzer, LogicalPlan, Optimizer}
import scraper.plans.physical.{LeafPhysicalPlan, PhysicalPlan, SingleRowRelation}

class TestContext extends Context {
  override type QueryExecution = TestQueryExecution

  override type Catalog = InMemoryCatalog

  override val settings: Settings = Test.defaultSettings

  override val catalog: Catalog = new InMemoryCatalog

  override def parse(query: String): LogicalPlan = new DeprecatedParser(settings).parse(query)

  private val analyzer = new Analyzer(catalog)

  override def analyze(plan: LogicalPlan): LogicalPlan = analyzer(plan)

  private val optimizer = new Optimizer

  override def optimize(plan: LogicalPlan): LogicalPlan = optimizer(plan)

  override def plan(plan: LogicalPlan): PhysicalPlan = TestPhysicalPlan(plan.output)

  override def execute(logicalPlan: LogicalPlan): QueryExecution =
    new TestQueryExecution(logicalPlan, this)
}

class TestQueryExecution(val logicalPlan: LogicalPlan, val context: Context) extends QueryExecution

case class TestPhysicalPlan(output: Seq[Attribute]) extends LeafPhysicalPlan {
  override def iterator: Iterator[Row] = Iterator.empty
}
