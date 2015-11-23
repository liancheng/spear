package scraper.expressions

import scala.util.Try

import scraper.expressions.Cast.promoteDataTypes
import scraper.types.{BooleanType, DataType}
import scraper.{Row, TypeMismatchException}

trait BinaryLogicalPredicate extends Predicate with BinaryExpression {
  override lazy val strictlyTyped: Try[Expression] = {
    for {
      lhs <- left.strictlyTyped map {
        case e: Predicate => e
        case e            => throw TypeMismatchException(e, BooleanType.getClass, None)
      }
      rhs <- right.strictlyTyped map {
        case e: Predicate => e
        case e            => throw TypeMismatchException(e, BooleanType.getClass, None)
      }
      (promotedLhs, promotedRhs) <- promoteDataTypes(lhs, rhs)
      newChildren = promotedLhs :: promotedRhs :: Nil
    } yield if (sameChildren(newChildren)) this else makeCopy(newChildren)
  }
}

case class And(left: Predicate, right: Predicate) extends BinaryLogicalPredicate {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = {
    lhs.asInstanceOf[Boolean] && rhs.asInstanceOf[Boolean]
  }

  override def nodeCaption: String =
    s"(${left.nodeCaption} AND ${right.nodeCaption})"
}

case class Or(left: Predicate, right: Predicate) extends BinaryLogicalPredicate {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any =
    lhs.asInstanceOf[Boolean] || rhs.asInstanceOf[Boolean]

  override def nodeCaption: String =
    s"(${left.nodeCaption} OR ${right.nodeCaption})"
}

case class Not(child: Predicate) extends UnaryPredicate {
  override def evaluate(input: Row): Any = !child.evaluate(input).asInstanceOf[Boolean]

  override def nodeCaption: String = s"(NOT ${child.nodeCaption})"

  override lazy val strictlyTyped: Try[Expression] = for {
    e <- child.strictlyTyped map {
      case e: Predicate => e
      case e            => throw TypeMismatchException(e, BooleanType.getClass, None)
    }
  } yield copy(child = e)
}

case class If(condition: Predicate, trueValue: Expression, falseValue: Expression)
  extends Expression {

  override def dataType: DataType = whenStrictlyTyped(trueValue.dataType)

  override def children: Seq[Expression] = Seq(condition, trueValue, falseValue)

  override def nodeCaption: String =
    s"if (${condition.nodeCaption}) " +
      s"${trueValue.nodeCaption} else " +
      s"${falseValue.nodeCaption}"

  override lazy val strictlyTyped: Try[Expression] = for {
    c <- condition.strictlyTyped map {
      case e: Predicate => e
      case e            => throw TypeMismatchException(e, BooleanType.getClass, None)
    }
    t <- trueValue.strictlyTyped
    f <- falseValue.strictlyTyped
    (promotedT, promotedF) <- promoteDataTypes(t, f)
    newChildren = c :: promotedT :: promotedF :: Nil
  } yield if (sameChildren(newChildren)) this else makeCopy(newChildren)

  override def evaluate(input: Row): Any =
    if (condition.evaluate(input).asInstanceOf[Boolean]) {
      trueValue.evaluate(input)
    } else {
      falseValue.evaluate(input)
    }
}
