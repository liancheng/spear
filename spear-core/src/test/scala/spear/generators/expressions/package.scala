package spear.generators

import scala.collection.immutable.Stream.Empty

import org.scalacheck.{Gen, Shrink}
import org.scalacheck.Shrink.shrink

import spear.config.Settings
import spear.exceptions.TypeMismatchException
import spear.expressions._
import spear.generators.values._
import spear.types.{BooleanType, FieldSpec, NumericType, PrimitiveType}
import spear.utils.Logging

package object expressions extends Logging {
  def genExpression(input: Seq[Expression], outputSpec: FieldSpec)(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case BooleanType      => genPredicate(input, outputSpec)
    case _: NumericType   => genArithmetic(input, outputSpec)
    case _: PrimitiveType => genLiteral(outputSpec)
  }

  def genArithmetic(input: Seq[Expression], outputSpec: FieldSpec)(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case _: NumericType =>
      genTermExpression(input, outputSpec)
  }

  def genTermExpression(input: Seq[Expression], outputSpec: FieldSpec)(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case _: NumericType =>
      for {
        size <- Gen.size

        genProduct = genProductExpression(input, outputSpec)
        genNegate = Gen.resize(size - 1, genProduct map Negate)
        genTerm = genUnaryOrBinary(genProduct, Plus, Minus)

        term <- size match {
          case 1 => genProduct
          case 2 => Gen.oneOf(genProduct, genNegate)
          case _ => Gen.oneOf(genProduct, genNegate, genTerm)
        }
      } yield term

    case BooleanType =>
      genPredicate(input, outputSpec)
  }

  def genProductExpression(input: Seq[Expression], outputSpec: FieldSpec)(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case _: NumericType =>
      Gen.sized {
        case size if size < 2 =>
          genBaseExpression(input, outputSpec)

        case size =>
          val genBranch = genBaseExpression(input, outputSpec)
          genUnaryOrBinary(genBranch, Multiply, Divide)
      }
  }

  def genBaseExpression(input: Seq[Expression], outputSpec: FieldSpec)(
    implicit
    settings: Settings
  ): Gen[Expression] = {
    val candidates = input.filter { e =>
      e.isNullable == outputSpec.nullable && e.dataType == outputSpec.dataType
    }

    val genLeaf = if (candidates.nonEmpty) {
      Gen.oneOf(candidates)
    } else {
      genLiteral(outputSpec)
    }

    Gen.sized {
      case 1 => genLeaf
      case _ => Gen.oneOf(genLeaf, Gen.lzy(genExpression(input, outputSpec)))
    }
  }

  def genPredicate(input: Seq[Expression], outputSpec: FieldSpec = BooleanType.?)(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case BooleanType => genOrExpression(input, outputSpec)
  }

  def genLogicalPredicate(input: Seq[Expression], outputSpec: FieldSpec = BooleanType.?)(
    implicit
    settings: Settings
  ): Gen[Expression] = genPredicate(input, outputSpec)(settings.withValue(
    Keys.OnlyLogicalOperatorsInPredicate, true
  ))

  def genOrExpression(input: Seq[Expression], outputSpec: FieldSpec = BooleanType.?)(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case BooleanType =>
      val genBranch = genAndExpression(input, outputSpec)
      genUnaryOrBinary(genBranch, Or)
  }

  def genAndExpression(input: Seq[Expression], outputSpec: FieldSpec = BooleanType.?)(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case BooleanType =>
      val genBranch = Gen.sized {
        case size if size < 2 =>
          genComparison(input, outputSpec)

        case _ if settings(Keys.OnlyLogicalOperatorsInPredicate) =>
          genNotExpression(input, outputSpec)

        case _ =>
          Gen.oneOf(
            genNotExpression(input, outputSpec),
            genComparison(input, outputSpec)
          )
      }

      genUnaryOrBinary(genBranch, And)
  }

  def genNotExpression(input: Seq[Expression], outputSpec: FieldSpec = BooleanType.?)(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case BooleanType =>
      for {
        size <- Gen.size
        predicate <- Gen.resize(size - 1, genPredicate(input, outputSpec))
      } yield Not(predicate)
  }

  def genComparison(input: Seq[Expression], outputSpec: FieldSpec = BooleanType.?)(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case BooleanType =>
      val genBoolLiteral = genLiteral(outputSpec.copy(dataType = BooleanType))
      val genBranch = Gen.lzy(genTermExpression(input, outputSpec))

      Gen.sized {
        case size if size < 2 =>
          genBoolLiteral

        case _ =>
          Gen.oneOf(
            genBinary(genBranch, Gt, GtEq, Lt, LtEq, Eq, NotEq),
            genBoolLiteral
          )
      }
  }

  def genLiteral(outputSpec: FieldSpec)(implicit settings: Settings): Gen[Literal] = {
    val (dataType, nullable) = outputSpec match {
      case FieldSpec(t: PrimitiveType, n) => (t, n)
      case FieldSpec(t, _) =>
        throw new TypeMismatchException(
          s"Literal only accepts primitive type while a ${t.sql} was found"
        )
    }

    val nullFreq = if (nullable) (settings(Keys.NullChances) * 100).toInt else 0
    val nonNullFreq = 100 - nullFreq

    Gen.frequency(
      nullFreq -> Gen.const(Literal(null, dataType)),
      nonNullFreq -> genValueForPrimitiveType(dataType).map(Literal(_, dataType))
    )
  }

  implicit lazy val shrinkByte: Shrink[Byte] = Shrink { n =>
    shrink(n.toInt) map { _.toByte }
  }

  implicit lazy val shrinkShort: Shrink[Short] = Shrink { n =>
    shrink(n.toInt) map { _.toShort }
  }

  implicit lazy val shrinkLong: Shrink[Long] = {
    def integralHalves[T: Integral](n: T): Stream[T] = {
      val i = implicitly[Integral[T]]
      if (i.compare(n, i.zero) == 0) Empty else n #:: integralHalves(i.quot(n, i.fromInt(2)))
    }

    Shrink { n =>
      if (n == 0) Empty else {
        val ns = integralHalves(n / 2) map { n - _ }
        0 #:: interleave(ns, ns map { -1 * _ })
      }
    }
  }

  implicit lazy val shrinkFloat: Shrink[Float] = shrinkFractional[Float]

  implicit lazy val shrinkDouble: Shrink[Double] = shrinkFractional[Double]

  private def shrinkFractional[T: Fractional]: Shrink[T] = {
    def fractionalHalves[U: Fractional](n: U): Stream[U] = {
      val f = implicitly[Fractional[U]]
      val epsilon = f.toInt(f.times(f.minus(n, f.zero), f.fromInt(100000000)))
      if (epsilon == 0) Empty else n #:: fractionalHalves(f.div(n, f.fromInt(2)))
    }

    Shrink { n =>
      val f = implicitly[Fractional[T]]
      val ns = fractionalHalves(f.div(n, f.fromInt(2))) map { f.minus(n, _) }
      f.zero #:: interleave(ns, ns map { f.times(_, f.fromInt(-1)) })
    }
  }

  private def interleave[T](xs: Stream[T], ys: Stream[T]): Stream[T] = (xs, ys) match {
    case (Empty, _) => ys
    case (_, Empty) => xs
    case _          => xs.head #:: ys.head #:: interleave(xs.tail, ys.tail)
  }

  lazy val shrinkLiteral: Shrink[Literal] = Shrink {
    case lit @ Literal(value: Byte, _)   => shrink(value) map { v => lit.copy(value = v) }
    case lit @ Literal(value: Short, _)  => shrink(value) map { v => lit.copy(value = v) }
    case lit @ Literal(value: Int, _)    => shrink(value) map { v => lit.copy(value = v) }
    case lit @ Literal(value: Long, _)   => shrink(value) map { v => lit.copy(value = v) }
    case lit @ Literal(value: Float, _)  => shrink(value) map { v => lit.copy(value = v) }
    case lit @ Literal(value: Double, _) => shrink(value) map { v => lit.copy(value = v) }
    case lit @ Literal(value: String, _) => shrink(value) map { v => lit.copy(value = v) }
    case _                               => Empty
  }

  implicit def shrinkExpression(implicit settings: Settings): Shrink[Expression] = Shrink {
    case lit: Literal =>
      shrinkLiteral.shrink(lit)

    case e =>
      def stripLeaves(e: Expression): Expression = e transformDown {
        case child if !child.isLeaf && child.children.forall(_.isLeaf) =>
          genLiteral(FieldSpec(child.dataType, child.isNullable)).sample.get
      }

      val compatibleChildren = e.children filter (_.dataType == e.dataType)
      compatibleChildren.toStream :+ stripLeaves(e)
  }

  private def genUnaryOrBinary[T <: Expression](genBranch: Gen[T], ops: ((T, T) => T)*): Gen[T] =
    Gen.sized {
      case size if size < 3 => genBranch
      case size             => Gen.oneOf(genBranch, genBinary(genBranch, ops: _*))
    }

  private def genBinary[T <: Expression, R <: Expression](
    genBranch: Gen[T], ops: ((T, T) => R)*
  ): Gen[R] = Gen.parameterized { params =>
    for {
      size <- Gen.size
      op <- Gen.oneOf(ops)

      lhsSize = params.rng.nextInt(size - 1)
      lhs <- Gen.resize(lhsSize, genBranch)

      rhsSize = size - 1 - lhsSize
      rhs <- Gen.resize(rhsSize, genBranch)
    } yield op(lhs, rhs)
  }
}
