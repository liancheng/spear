package scraper.expressions

import scraper.parser.Parser

package object functions {
  def col(name: String): UnresolvedAttribute = (new Parser).parseAttribute(name)

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

  def count(): Count = Count(Star(None))

  def first(expression: Expression): FirstValue = FirstValue(expression)

  def last(expression: Expression): LastValue = LastValue(expression)

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

  def array(first: Expression, rest: Expression*): CreateArray = CreateArray(first +: rest)

  def map(keyValues: Expression*): CreateMap = {
    require(keyValues.length % 2 == 0)
    val (keys, values) = keyValues.sliding(2, 2).map {
      case Seq(key, value) => key -> value
    }.toSeq.unzip
    CreateMap(keys, values)
  }
}
