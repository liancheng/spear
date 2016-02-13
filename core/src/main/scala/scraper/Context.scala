package scraper

import scraper.config.Settings
import scraper.expressions.Expression
import scraper.plans.logical.{LogicalPlan, SingleRowRelation}
import scraper.plans.physical.PhysicalPlan

trait Catalog {
  def registerRelation(tableName: String, analyzedPlan: LogicalPlan): Unit

  def removeRelation(tableName: String): Unit

  def lookupRelation(tableName: String): LogicalPlan
}

trait Context {
  type QueryExecution <: plans.QueryExecution

  type Catalog <: scraper.Catalog

  def settings: Settings

  def catalog: Catalog

  /**
   * Parses given query string to a [[LogicalPlan]].
   */
  def parse(query: String): LogicalPlan

  /**
   * Analyzes unresolved [[LogicalPlan]] into strictly-typed [[LogicalPlan]].
   */
  def analyze(plan: LogicalPlan): LogicalPlan

  /**
   * Optimizes a [[LogicalPlan]] into another equivalent but more performant version.
   */
  def optimize(plan: LogicalPlan): LogicalPlan

  /**
   * Plans a [[LogicalPlan]] into an executable [[PhysicalPlan]].
   */
  def plan(plan: LogicalPlan): PhysicalPlan

  def execute(logicalPlan: LogicalPlan): QueryExecution

  lazy val single: DataFrame = new DataFrame(SingleRowRelation, this)

  def single(first: Expression, rest: Expression*): DataFrame = single select first +: rest

  def q(query: String): DataFrame

  def table(name: String): DataFrame
}

object Context {
  implicit class QueryString(query: String)(implicit context: Context) {
    def q: DataFrame = context q query
  }
}
