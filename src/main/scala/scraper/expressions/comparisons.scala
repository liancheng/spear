package scraper.expressions

import scala.util.{Failure, Success, Try}
import scalaz.Applicative

import scraper.Row
import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.promoteDataType
import scraper.types.{BooleanType, DataType, OrderedType}
import scraper.utils._

trait BinaryComparison extends BinaryOperator {
  override def dataType: DataType = BooleanType

  protected lazy val ordering: Ordering[Any] = whenStrictlyTyped {
    left.dataType match {
      case t: OrderedType => t.genericOrdering
    }
  }

  override lazy val strictlyTypedForm: Try[Expression] = for {
    lhs <- left.strictlyTypedForm map {
      case OrderedType(e) => e
      case e              => throw new TypeMismatchException(e, classOf[OrderedType])
    }

    rhs <- right.strictlyTypedForm map {
      case OrderedType(e) => e
      case e              => throw new TypeMismatchException(e, classOf[OrderedType])
    }

    t <- lhs.dataType widest rhs.dataType

    newChildren = promoteDataType(lhs, t) :: promoteDataType(rhs, t) :: Nil
  } yield if (sameChildren(newChildren)) this else makeCopy(newChildren)
}

object BinaryComparison {
  def unapply(e: Expression): Option[(Expression, Expression)] = e match {
    case c: BinaryComparison => Some((c.left, c.right))
    case _                   => None
  }
}

case class Eq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.equiv(lhs, rhs)

  override def operator: String = "="
}

case class NotEq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = !ordering.equiv(lhs, rhs)

  override def operator: String = "!="
}

case class Gt(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.gt(lhs, rhs)

  override def operator: String = ">"
}

case class Lt(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.lt(lhs, rhs)

  override def operator: String = "<"
}

case class GtEq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.gteq(lhs, rhs)

  override def operator: String = ">="
}

case class LtEq(left: Expression, right: Expression) extends BinaryComparison {
  override def nullSafeEvaluate(lhs: Any, rhs: Any): Any = ordering.lteq(lhs, rhs)

  override def operator: String = "<="
}

case class In(test: Expression, list: Seq[Expression]) extends Expression {
  override def children: Seq[Expression] = test +: list

  override protected def strictDataType: DataType = BooleanType

  override def strictlyTypedForm: Try[Expression] = {
    for {
      strictTest <- test.strictlyTypedForm
      strictList <- sequence(list map (_.strictlyTypedForm))

      testType = strictTest.dataType

      promotedList <- sequence(strictList map {
        case e if e.dataType narrowerThan testType => Success(promoteDataType(e, testType))
        case e => Failure(new TypeMismatchException(
          "Test value and list values must be of the same data type in IN expression."
        ))
      })

      newChildren = strictTest +: promotedList
    } yield if (sameChildren(newChildren)) this else copy(test = strictTest, list = promotedList)
  }

  override def evaluate(input: Row): Any = {
    val testValue = test evaluate input
    val listValues = list map (_ evaluate input)

    dataType match {
      case t: OrderedType =>
        listValues exists (t.genericOrdering.compare(testValue, _) == 0)

      case _ =>
        false
    }
  }

  override protected def template[T[_]: Applicative](f: Expression => T[String]): T[String] = {
    import scalaz.Scalaz._

    sequence(children map f) map {
      case Seq(testStr, listStr @ _*) =>
        s"($testStr IN (${listStr mkString ", "}))"
    }
  }
}
