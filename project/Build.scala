import scalariform.formatter.preferences.PreferencesImporterExporter

import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys.preferences
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import net.virtualvoid.sbt.graph.{Plugin => SbtDependencyGraph}
import pl.project13.scala.sbt.JmhPlugin
import sbt.Keys._
import sbt._
import scoverage.ScoverageSbtPlugin

object Build extends sbt.Build {
  lazy val scraper =
    Project("scraper", file("."))
      .aggregate(core, localExecution, repl)
      .settings(commonSettings)

  lazy val core =
    Project("core", file("core"))
      .enablePlugins(sbtPlugins: _*)
      .settings(commonSettings)
      .settings(libraryDependencies ++= coreDependencies)
      // Explicitly overrides all conflicting transitive dependencies
      .settings(dependencyOverrides ++= Dependencies.overrides)

  lazy val localExecution =
    Project("execution-local", file("execution/local"))
      .dependsOn(core % "compile->compile;test->test")
      .enablePlugins(sbtPlugins: _*)
      .settings(commonSettings)
      .settings(libraryDependencies ++= localExecutionDependencies)
      // Explicitly overrides all conflicting transitive dependencies
      .settings(dependencyOverrides ++= Dependencies.overrides)

  lazy val repl =
    Project("repl", file("repl"))
      .dependsOn(core % "compile->compile;test->test")
      .dependsOn(localExecution % "compile->compile;test->test")
      .enablePlugins(sbtPlugins: _*)
      .settings(commonSettings)
      .settings(libraryDependencies ++= Dependencies.ammonite)
      // Explicitly overrides all conflicting transitive dependencies
      .settings(dependencyOverrides ++= Dependencies.overrides)
      .settings(unmanagedClasspath in Runtime += baseDirectory.value.getParentFile / "conf")

  lazy val coreDependencies = {
    import Dependencies._
    test ++ config ++ log4j ++ scala ++ scopt ++ slf4j
  }

  lazy val localExecutionDependencies = {
    import Dependencies._
    test
  }

  lazy val sbtPlugins = Seq(
    // For packaging
    JavaAppPackaging,
    // For JMH benchmarking
    JmhPlugin,
    // For Scala code formatting
    SbtScalariform,
    // For Scala test coverage reporting
    ScoverageSbtPlugin
  )

  lazy val commonSettings =
    basicSettings ++ generalDependencySettings ++ scalariformSettings

  lazy val basicSettings = Seq(
    organization := "scraper",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := Dependencies.Versions.scala,
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
    javacOptions ++= Seq("-source", "1.7", "-target", "1.7", "-g"),
    fork := false,
    parallelExecution in Test := false,
    // Shows duration and full exception stack trace
    testOptions in Test += Tests.Argument("-oDF")
  )

  lazy val generalDependencySettings =
    SbtDependencyGraph.graphSettings ++ Seq(
      // Does not copy managed dependencies into `lib_managed`
      retrieveManaged := false,
      // Enables extra resolvers
      resolvers ++= Dependencies.extraResolvers,
      // Disables auto conflict resolution
      conflictManager := ConflictManager.strict
    )

  lazy val scalariformSettings =
    SbtScalariform.scalariformSettings ++ Seq(
      preferences := PreferencesImporterExporter.loadPreferences("scalariform.properties")
    )
}

object Dependencies {
  val extraResolvers = Seq(
    Resolver.mavenLocal,
    "Twitter Maven" at "http://maven.twttr.com"
  )

  object Versions {
    val ammonite = "0.6.0"
    val config = "1.2.1"
    val jline = "2.12.1"
    val log4j = "1.2.16"
    val protobuf = "2.5.0"
    val scala = "2.11.7"
    val scalaCheck = "1.12.5"
    val scalaParserCombinators = "1.0.4"
    val scalaXml = "1.0.4"
    val scalaTest = "2.2.5"
    val scopt = "3.4.0"
    val slf4j = "1.6.4"
  }

  val ammonite = Seq(
    "com.lihaoyi" % "ammonite-repl_2.11.7" % Versions.ammonite
  )

  val config = Seq(
    "com.typesafe" % "config" % Versions.config
  )

  val jline = Seq(
    "jline" % "jline" % Versions.jline
  )

  val log4j = Seq(
    "log4j" % "log4j" % Versions.log4j
  )

  val protobuf = Seq(
    "com.google.protobuf" % "protobuf-java" % Versions.protobuf
  )

  val scala = Seq(
    "org.scala-lang" % "scala-library" % Versions.scala,
    "org.scala-lang" % "scala-reflect" % Versions.scala,
    "org.scala-lang.modules" %% "scala-parser-combinators" % Versions.scalaParserCombinators,
    "org.scala-lang.modules" %% "scala-xml" % Versions.scalaXml
  )

  val scalaCheck = Seq(
    "org.scalacheck" %% "scalacheck" % Versions.scalaCheck % "test"
  )

  val scalaTest = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalaTest % "test"
  )

  val scopt = Seq(
    "com.github.scopt" %% "scopt" % Versions.scopt
  )

  val slf4j = Seq(
    "org.slf4j" % "slf4j-api" % Versions.slf4j,
    "org.slf4j" % "slf4j-log4j12" % Versions.slf4j,
    "org.slf4j" % "jul-to-slf4j" % Versions.slf4j
  )

  val test = scalaCheck ++ scalaTest

  val overrides = Set.empty ++ jline ++ protobuf ++ scala
}
