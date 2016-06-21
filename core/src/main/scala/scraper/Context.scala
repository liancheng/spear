package scraper

import scala.collection.Iterable
import scala.language.existentials
import scala.reflect.runtime.universe.WeakTypeTag

import scraper.config.Settings
import scraper.expressions._
import scraper.plans.logical.{LocalRelation, LogicalPlan, SingleRowRelation}
import scraper.plans.physical.PhysicalPlan
import scraper.types.{LongType, StructType}

trait Context {
  type QueryExecution <: plans.QueryExecution

  type Catalog <: scraper.Catalog

  def settings: Settings

  def catalog: Catalog

  /**
   * Parses given query string to a [[scraper.plans.logical.LogicalPlan logical plan]].
   */
  def parse(query: String): LogicalPlan

  /**
   * Analyzes an unresolved [[scraper.plans.logical.LogicalPlan logical plan]] and outputs its
   * strictly-typed version.
   */
  def analyze(plan: LogicalPlan): LogicalPlan

  /**
   * Optimizes a resolved [[scraper.plans.logical.LogicalPlan logical plan]] into another equivalent
   * but more performant version.
   */
  def optimize(plan: LogicalPlan): LogicalPlan

  /**
   * Plans a [[scraper.plans.logical.LogicalPlan logical plan]] into an executable
   * [[scraper.plans.physical.PhysicalPlan physical plan]].
   */
  def plan(plan: LogicalPlan): PhysicalPlan

  def execute(logicalPlan: LogicalPlan): QueryExecution

  private lazy val values: DataFrame = new DataFrame(SingleRowRelation, this)

  def values(first: Expression, rest: Expression*): DataFrame = values select first +: rest

  def q(query: String): DataFrame = new DataFrame(parse(query), this)

  def table(name: Name): DataFrame =
    new DataFrame(catalog lookupRelation name, this)

  def table(name: String): DataFrame = table(Name.cs(name))

  def table(name: Symbol): DataFrame = table(Name.ci(name.name))

  def lift[T <: Product: WeakTypeTag](data: Iterable[T]): DataFrame =
    new DataFrame(LocalRelation(data), this)

  def lift[T <: Product: WeakTypeTag](first: T, rest: T*): DataFrame = lift(first +: rest)

  def range(end: Long): DataFrame = range(0, end)

  def range(begin: Long, end: Long): DataFrame = range(begin, end, 1L)

  def range(begin: Long, end: Long, step: Long): DataFrame = {
    val rows = begin until end by step map (Row apply _)
    val output = StructType('id -> LongType.!).toAttributes
    new DataFrame(LocalRelation(rows, output), this)
  }
}

object Context {
  implicit class QueryString(query: String)(implicit context: Context) {
    def q: DataFrame = context q query
  }
}
