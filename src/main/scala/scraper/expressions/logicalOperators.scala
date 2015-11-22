package scraper.expressions

import scala.util.Try

import scraper.Row
import scraper.expressions.Cast.promoteDataTypes

trait BinaryLogicalPredicate extends Predicate with BinaryExpression {
  override lazy val strictlyTyped: Try[Expression] = {
    for {
      Predicate(lhs) <- left.strictlyTyped
      Predicate(rhs) <- right.strictlyTyped
      (e1, e2) <- promoteDataTypes(lhs, rhs)
    } yield makeCopy(e1 :: e2 :: Nil)
  }
}

case class And(left: Predicate, right: Predicate) extends BinaryLogicalPredicate {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = {
    lhs.asInstanceOf[Boolean] && rhs.asInstanceOf[Boolean]
  }

  override def caption: String = s"(${left.caption} AND ${right.caption})"
}

case class Or(left: Predicate, right: Predicate) extends BinaryLogicalPredicate {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = {
    lhs.asInstanceOf[Boolean] || rhs.asInstanceOf[Boolean]
  }

  override def caption: String = s"(${left.caption} OR ${right.caption})"
}

case class Not(child: Predicate) extends UnaryPredicate {
  override def evaluate(input: Row): Any = !child.evaluate(input).asInstanceOf[Boolean]

  override def caption: String = s"(NOT ${child.caption})"

  override lazy val strictlyTyped: Try[Expression] = for {
    Predicate(e) <- child.strictlyTyped
  } yield copy(child = e)
}
