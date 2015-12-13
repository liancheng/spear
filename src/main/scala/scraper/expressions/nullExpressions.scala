package scraper.expressions

import scala.util.Try

import scraper.Row
import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.{promoteDataType, widestTypeOf}
import scraper.types._

case class Coalesce(children: Seq[Expression]) extends Expression {
  override def dataType: DataType = whenStrictlyTyped(children.head.dataType)

  override lazy val strictlyTypedForm: Try[Expression] = for {
    strictChildren <- Try(children map (_.strictlyTypedForm.get))
    finalType <- widestTypeOf(strictChildren.map(_.dataType))
    promotedChildren = children.map(promoteDataType(_, finalType))
  } yield if (sameChildren(promotedChildren)) this else copy(children = promotedChildren)

  override def sql: String = s"COALESCE(${children map (_.sql) mkString ", "})"

  override def annotatedString: String =
    s"COALESCE(${children map (_.annotatedString) mkString ", "})"

  override def evaluate(input: Row): Any =
    (children.iterator map (_ evaluate input) find (_ != null)).orNull
}

case class IsNull(child: Expression) extends UnaryExpression {
  override def strictlyTypedForm: Try[Expression] = for {
    strictChild <- child.strictlyTypedForm
  } yield if (strictChild sameOrEqual child) this else copy(child = strictChild)

  override def sql: String = s"(${child.sql} IS NULL)"

  override def evaluate(input: Row): Any = (child evaluate input) == null

  override def dataType: DataType = BooleanType

  override def annotatedString: String = s"(${child.sql} IS NULL)"
}

case class IsNotNull(child: Expression) extends UnaryExpression {
  override def strictlyTypedForm: Try[Expression] = for {
    strictChild <- child.strictlyTypedForm
  } yield if (strictChild sameOrEqual child) this else copy(child = strictChild)

  override def sql: String = s"(${child.sql} IS NOT NULL)"

  override def evaluate(input: Row): Any = (child evaluate input) != null

  override def dataType: DataType = BooleanType

  override def annotatedString: String = s"(${child.annotatedString} IS NOT NULL)"
}

case class IsNaN(child: Expression) extends UnaryExpression {
  override def strictlyTypedForm: Try[Expression] = for {
    strictChild <- child.strictlyTypedForm map {
      case FractionalType(e)            => e
      case FractionalType.Implicitly(e) => e
      case e =>
        throw new TypeMismatchException(e, classOf[FractionalType])
    }

    finalType = strictChild.dataType match {
      case t: FractionalType            => t
      case FractionalType.Implicitly(t) => FractionalType.defaultType
    }

    promotedChild = promoteDataType(strictChild, finalType)
  } yield if (promotedChild sameOrEqual child) this else copy(child = promotedChild)

  override def dataType: DataType = BooleanType

  override def sql: String = s"ISNAN(${child.sql})"

  override def annotatedString: String = s"ISNAN(${child.annotatedString})"

  override def evaluate(input: Row): Any = {
    val value = child evaluate input
    if (value == null) false else dataType match {
      case DoubleType => value.asInstanceOf[Double].isNaN
      case FloatType  => value.asInstanceOf[Float].isNaN
      case _          => false
    }
  }
}
