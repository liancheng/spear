package scraper.expressions

import scala.util.Try

import scraper.expressions.Cast.promoteDataTypes
import scraper.types.PrimitiveType

trait BinaryComparison extends Predicate with BinaryExpression {
  protected lazy val ordering: Ordering[Any] = whenStrictlyTyped(left.dataType match {
    case t: PrimitiveType => t.ordering.asInstanceOf[Ordering[Any]]
  })

  override lazy val strictlyTyped: Try[Expression] = for {
    PrimitiveType(lhs) <- left.strictlyTyped
    PrimitiveType(rhs) <- right.strictlyTyped
    (promotedLhs, promotedRhs) <- promoteDataTypes(lhs, rhs)
    newChildren = promotedLhs :: promotedRhs :: Nil
  } yield if (sameChildren(newChildren)) this else makeCopy(newChildren)
}

case class Eq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = lhs == rhs

  override def nodeCaption: String =
    s"(${left.nodeCaption} = ${right.nodeCaption})"
}

case class NotEq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = lhs != rhs

  override def nodeCaption: String =
    s"(${left.nodeCaption} != ${right.nodeCaption})"
}

case class Gt(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.gt(lhs, rhs)

  override def nodeCaption: String =
    s"(${left.nodeCaption} > ${right.nodeCaption}"
}

case class Lt(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.lt(lhs, rhs)

  override def nodeCaption: String =
    s"(${left.nodeCaption} < ${right.nodeCaption}"
}

case class GtEq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.gteq(lhs, rhs)

  override def nodeCaption: String =
    s"(${left.nodeCaption} >= ${right.nodeCaption}"
}

case class LtEq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.lteq(lhs, rhs)

  override def nodeCaption: String =
    s"(${left.nodeCaption} <= ${right.nodeCaption}"
}
