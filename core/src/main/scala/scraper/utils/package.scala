package scraper

import scala.language.{higherKinds, postfixOps}
import scala.util.Try
import scalaz.Scalaz._
import scalaz._

import com.typesafe.config.{Config, ConfigFactory}

package object utils {
  def sideBySide(lhs: String, rhs: String, withHeader: Boolean): String = {
    val lhsLines = lhs split "\n"
    val rhsLines = rhs split "\n"

    val lhsWidth = lhsLines map (_.length) max
    val rhsWidth = rhsLines map (_.length) max

    val height = lhsLines.length max rhsLines.length
    val paddedLhs = lhsLines map (_ padTo (lhsWidth, ' ')) padTo (height, " " * lhsWidth)
    val paddedRhs = rhsLines map (_ padTo (rhsWidth, ' ')) padTo (height, " " * rhsWidth)

    val zipped = paddedLhs zip paddedRhs
    val header = if (withHeader) zipped take 1 else Array.empty[(String, String)]
    val contents = if (withHeader) zipped drop 1 else zipped

    def rtrim(str: String): String = str.replaceAll("\\s+$", "")

    header.map {
      case (lhsLine, rhsLine) =>
        rtrim(s"# $lhsLine : $rhsLine")
    } ++ contents.map {
      case (lhsLine, rhsLine) =>
        val diffIndicator = if (rtrim(lhsLine) != rtrim(rhsLine)) "!" else " "
        rtrim(s"$diffIndicator $lhsLine : $rhsLine")
    }
  } mkString ("\n", "\n", "")

  def loadConfig(component: String): Config =
    ConfigFactory
      // Environment variables takes highest priority and overrides everything else
      .systemEnvironment()
      // System properties comes after environment variables
      .withFallback(ConfigFactory.systemProperties())
      // Then follows user provided configuration files
      .withFallback(ConfigFactory.parseResources(s"scraper-$component.conf"))
      // Reference configuration, bundled as resource
      .withFallback(ConfigFactory.parseResources(s"scraper-$component-reference.conf"))
      // Configurations of all other components (like Akka)
      .withFallback(ConfigFactory.load())
      .resolve()

  implicit class StraightString(string: String) {
    def straight: String = straight('|', " ")

    def straight(joiner: String): String =
      string stripMargin '|' split "\n" mkString joiner

    def straight(marginChar: Char, joiner: String): String =
      string stripMargin marginChar split "\n" mkString joiner trim
  }

  /**
   * Makes `Try[T]` an `Applicative`, so that we can apply [[sequence]] over it.
   */
  class TryApplicative[T] extends Applicative[Try] {
    override def point[A](a: => A): Try[A] = Try(a)

    override def ap[A, B](fa: => Try[A])(f: => Try[(A) => B]): Try[B] =
      for (a <- fa; fn <- f) yield fn(a)
  }

  implicit def `Try[T]->Applicative[Try]`[T]: Applicative[Try] = new TryApplicative[T]

  def sequence[F[_]: Applicative, A](seq: Seq[F[A]]): F[Seq[A]] = seq match {
    case xs if xs.isEmpty => Seq.empty[A].point[F]
    case Seq(x, xs @ _*)  => (x |@| sequence(xs)) { _ +: _ }
  }

  def quote(name: String): String = "`" + name.replace("`", "``") + "`"
}
