package scraper

import scala.language.postfixOps

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
        rtrim(s"# $lhsLine | $rhsLine")
    } ++ contents.map {
      case (lhsLine, rhsLine) =>
        val diffIndicator = if (rtrim(lhsLine) != rtrim(rhsLine)) ">" else " "
        rtrim(s"$diffIndicator $lhsLine | $rhsLine")
    }
  } mkString "\n"

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
}
