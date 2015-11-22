package scraper.expressions

import scala.util.Try

import scraper.expressions.Cast.promoteDataTypes
import scraper.types.PrimitiveType

trait BinaryComparison extends Predicate with BinaryExpression {
  protected lazy val ordering: Ordering[Any] = whenStrictlyTyped(left.dataType match {
    case t: PrimitiveType => t.ordering.asInstanceOf[Ordering[Any]]
  })

  override lazy val strictlyTyped: Try[this.type] = for {
    lhs <- left.strictlyTyped if lhs.dataType.isInstanceOf[PrimitiveType]
    rhs <- right.strictlyTyped if rhs.dataType.isInstanceOf[PrimitiveType]
    (e1, e2) <- promoteDataTypes(lhs, rhs)
  } yield makeCopy(e1 :: e2 :: Nil)
}

case class Eq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = lhs == rhs

  override def caption: String = s"(${left.caption} = ${right.caption})"
}

case class NotEq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = lhs != rhs

  override def caption: String = s"(${left.caption} != ${right.caption})"
}

case class Gt(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.gt(lhs, rhs)

  override def caption: String = s"(${left.caption} > ${right.caption}"
}

case class Lt(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.lt(lhs, rhs)

  override def caption: String = s"(${left.caption} < ${right.caption}"
}

case class GtEq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.gteq(lhs, rhs)

  override def caption: String = s"(${left.caption} >= ${right.caption}"
}

case class LtEq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.lteq(lhs, rhs)

  override def caption: String = s"(${left.caption} <= ${right.caption}"
}
