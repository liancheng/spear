package scraper.plans

trait QueryPlanner[Logical <: QueryPlan[Logical], Physical <: QueryPlan[Physical]] {
  trait Strategy {
    def apply(logicalPlan: Logical): Seq[Physical]
  }

  def strategies: Seq[Strategy]

  def planLater(logicalPlan: Logical): Physical = plan(logicalPlan)

  def plan(logicalPlan: Logical): Physical = {
    val physicalPlans = for {
      strategy <- strategies
      physicalPlan <- strategy(logicalPlan)
    } yield physicalPlan

    assert(physicalPlans.nonEmpty, s"No plans for $logicalPlan")
    physicalPlans.head
  }
}
