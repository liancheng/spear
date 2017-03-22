package scraper

import org.mockito.Mockito._

import scraper.parsers.QueryExpressionParser.queryExpression
import scraper.plans.QueryExecution
import scraper.plans.logical.{LogicalPlan, Optimizer}
import scraper.plans.logical.analysis.Analyzer
import scraper.plans.physical.PhysicalPlan

class TestQueryExecutor extends QueryExecutor {
  override val catalog: Catalog = new InMemoryCatalog

  override def parse(query: String): LogicalPlan = queryExpression.parse(query).get.value

  override def analyze(plan: LogicalPlan): LogicalPlan = analyzer apply plan

  override def optimize(plan: LogicalPlan): LogicalPlan = optimizer(plan)

  override def plan(plan: LogicalPlan): PhysicalPlan = {
    val mockedPhysicalPlan = mock(classOf[PhysicalPlan])
    when(mockedPhysicalPlan.iterator).thenReturn(Iterator.empty)
    mockedPhysicalPlan
  }

  override def execute(context: Context, plan: LogicalPlan): QueryExecution =
    new QueryExecution(context, plan)

  private val analyzer = new Analyzer(catalog)

  private val optimizer = new Optimizer
}
