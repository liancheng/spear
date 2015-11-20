import scalariform.formatter.preferences._

import scalariform.formatter.preferences.PreferencesImporterExporter
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys.preferences
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import net.virtualvoid.sbt.graph.{ Plugin => SbtDependencyGraph }
import pl.project13.scala.sbt.JmhPlugin
import sbt.Keys._
import sbt._
import scoverage.ScoverageSbtPlugin

object Build extends sbt.Build {
  lazy val scraper =
    Project("scraper", file("."))
      // For packaging
      .enablePlugins(JavaAppPackaging)
      // For JMH benchmarking
      .enablePlugins(JmhPlugin)
      // For Scala code formatting
      .enablePlugins(SbtScalariform)
      // For Scala test coverage reporting
      .enablePlugins(ScoverageSbtPlugin)
      .settings(basicSettings)
      .settings(dependencySettings)
      .settings(scalariformSettings)

  lazy val basicSettings =
    Seq(
      organization := "scraper",
      version := "0.1.0-SNAPSHOT",
      scalaVersion := Dependencies.Versions.scala,
      scalacOptions ++= Seq("-unchecked", "-deprecation"),
      javacOptions ++= Seq("-source", "1.7", "-target", "1.7", "-g"),
      fork := false,
      parallelExecution in Test := false
    )

  lazy val dependencySettings =
    SbtDependencyGraph.graphSettings ++ Seq(
      // Does not copy managed dependencies into `lib_managed`
      retrieveManaged := false,
      // Enables extra resolvers
      resolvers ++= Dependencies.extraResolvers,
      // Specifies dependencies
      libraryDependencies ++= Dependencies.all,
      // Disables auto conflict resolution
      conflictManager := ConflictManager.strict,
      // Explicitly overrides all conflicting transitive dependencies
      dependencyOverrides ++= Dependencies.overrides
    )

  lazy val scalariformSettings =
    SbtScalariform.scalariformSettings ++ Seq(
      preferences := PreferencesImporterExporter.loadPreferences("scalariform.properties")
    )
}

object Dependencies {
  val extraResolvers = Seq(
    Resolver.mavenLocal
  )

  object Versions {
    val config = "1.3.0"
    val jline = "2.13"
    val log4j = "1.2.16"
    val parquetMr = "1.8.1"
    val protobuf = "2.5.0"
    val scala = "2.11.7"
    val scalaCheck = "1.12.5"
    val scalaParserCombinators = "1.0.4"
    val scalaTest = "2.2.5"
    val scopt = "3.3.0"
    val slf4j = "1.6.4"
  }

  val config = Seq(
    "com.typesafe" % "config" % Versions.config
  )

  val jline = Seq(
    "jline" % "jline" % Versions.jline
  )

  val log4j = Seq(
    "log4j" % "log4j" % Versions.log4j
  )

  val parquetMr = Seq(
    "org.apache.parquet" % "parquet-avro" % Versions.parquetMr,
    "org.apache.parquet" % "parquet-thrift" % Versions.parquetMr,
    "org.apache.parquet" % "parquet-protobuf" % Versions.parquetMr,
    "org.apache.parquet" % "parquet-hive" % Versions.parquetMr
  )

  val protobuf = Seq(
    "com.google.protobuf" % "protobuf-java" % Versions.protobuf
  )

  val scala = Seq(
    "org.scala-lang" % "scala-library" % Versions.scala,
    "org.scala-lang" % "scala-reflect" % Versions.scala,
    "org.scala-lang.modules" %% "scala-parser-combinators" % Versions.scalaParserCombinators
  )

  val scalaTest = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalaTest % "test",
    "org.scalacheck" %% "scalacheck" % Versions.scalaCheck % "test"
  )

  val scopt = Seq(
    "com.github.scopt" %% "scopt" % Versions.scopt
  )

  val slf4j = Seq(
    "org.slf4j" % "slf4j-api" % Versions.slf4j,
    "org.slf4j" % "slf4j-log4j12" % Versions.slf4j,
    "org.slf4j" % "jul-to-slf4j" % Versions.slf4j
  )

  val test = Seq.empty[ModuleID]

  val all = test ++ config ++ jline ++ log4j ++ parquetMr ++ scala ++ scopt ++ scalaTest ++ slf4j

  val overrides = Set.empty ++ protobuf ++ scala
}
