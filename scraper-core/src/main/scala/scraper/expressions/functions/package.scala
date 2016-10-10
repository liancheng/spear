package scraper.expressions

import scraper.expressions.aggregates._

package object functions {
  def lit(value: Any): Literal = Literal(value)

  def not(predicate: Expression): Not = Not(predicate)

  def when(condition: Expression, consequence: Expression): CaseWhen =
    CaseWhen(condition :: Nil, consequence :: Nil, None)

  def coalesce(first: Expression, second: Expression, rest: Expression*): Coalesce =
    Coalesce(Seq(first, second) ++ rest)

  def rand(seed: Int): Rand = Rand(seed)

  // -------------------
  // Aggregate functions
  // -------------------

  def count(expression: Expression): Count = Count(expression)

  def count(): Count = Count(*)

  def first(expression: Expression, ignoresNull: Boolean = true): First =
    First(expression, ignoresNull)

  def last(expression: Expression, ignoresNull: Boolean = true): Last =
    Last(expression, ignoresNull)

  def average(expression: Expression): Average = Average(expression)

  def avg(expression: Expression): Average = average(expression)

  def sum(expression: Expression): Sum = Sum(expression)

  def product(expression: Expression): Product_ = Product_(expression)

  def max(expression: Expression): Max = Max(expression)

  def min(expression: Expression): Min = Min(expression)

  def bool_and(expression: Expression): BoolAnd = BoolAnd(expression)

  def bool_or(expression: Expression): BoolOr = BoolOr(expression)

  def collect_list(child: Expression): CollectList = CollectList(child)

  def collect_set(child: Expression): CollectSet = CollectSet(child)

  def distinct(agg: AggregateFunction): DistinctAggregateFunction = DistinctAggregateFunction(agg)

  // ----------------
  // String functions
  // ----------------

  def concat(expressions: Seq[Expression]): Concat = Concat(expressions)

  def concat(first: Expression, rest: Expression*): Concat = Concat(first +: rest)

  def rlike(string: Expression, pattern: Expression): RLike = RLike(string, pattern)

  def rlike(string: Expression, pattern: String): RLike = RLike(string, pattern)

  // -------------------------
  // Complex type constructors
  // -------------------------

  def named_struct(
    first: (Expression, Expression), rest: (Expression, Expression)*
  ): CreateNamedStruct = {
    val (names, values) = (first +: rest).unzip
    CreateNamedStruct(names, values)
  }

  def struct(first: Expression, rest: Expression*): CreateNamedStruct = struct(first +: rest)

  def struct(args: Seq[Expression]): CreateNamedStruct = {
    val fieldNames = args.indices.map("c" + _).map(lit)
    CreateNamedStruct(fieldNames, args)
  }

  def array(first: Expression, rest: Expression*): CreateArray = CreateArray(first +: rest)

  def map(keyValues: Expression*): CreateMap = {
    require(keyValues.length % 2 == 0)
    val (keys, values) = keyValues.sliding(2, 2).map {
      case Seq(key, value) => key -> value
    }.toSeq.unzip
    CreateMap(keys, values)
  }
}
