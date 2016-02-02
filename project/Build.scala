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
      // For packaging
      .enablePlugins(JavaAppPackaging)
      // For JMH benchmarking
      .enablePlugins(JmhPlugin)
      // For Scala code formatting
      .enablePlugins(SbtScalariform)
      // For Scala test coverage reporting
      .enablePlugins(ScoverageSbtPlugin)
      .settings(basicSettings)
      .settings(consoleSettings)
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
      parallelExecution in Test := false,
      // Shows duration and full exception stack trace
      testOptions in Test += Tests.Argument("-oDF")
    )

  lazy val consoleSettings =
    Seq(
      initialCommands in console :=
        """import scraper.LocalContext
          |import scraper.expressions.dsl._
          |import scraper.expressions.functions._
          |import scraper.types._
          |
          |val context = new LocalContext
          |""".stripMargin,

      initialCommands in console in Test :=
        """import org.scalacheck.Gen
          |import org.scalacheck.Gen._
          |import org.scalacheck.Prop
          |import org.scalacheck.Prop._
          |import org.scalacheck.Test
          |import org.scalacheck.Test._
          |
          |import scraper.LocalContext
          |import scraper.expressions.dsl._
          |import scraper.expressions.functions._
          |import scraper._
          |
          |import scraper.Test._
          |import scraper.types._
          |import scraper.generators.expressions._
          |import scraper.generators.plans.logical._
          |import scraper.generators.types._
          |import scraper.generators.values._
          |
          |val context = new LocalContext
          |""".stripMargin
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
    Resolver.mavenLocal,
    "Twitter Maven" at "http://maven.twttr.com"
  )

  object Versions {
    val config = "1.2.1"
    val janino = "2.7.8"
    val log4j = "1.2.16"
    val parquetMr = "1.8.1"
    val protobuf = "2.5.0"
    val scala = "2.11.7"
    val scalaCheck = "1.12.5"
    val scalaParserCombinators = "1.0.4"
    val scalaXml = "1.0.4"
    val scalaTest = "2.2.5"
    val scalaz = "7.2.0"
    val scodecCore = "1.8.3"
    val scopt = "3.3.0"
    val shapeless = "2.2.5"
    val slf4j = "1.6.4"
  }

  val config = Seq(
    "com.typesafe" % "config" % Versions.config
  )

  val janino = Seq(
    "org.codehaus.janino" % "janino" % Versions.janino
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
    "org.scala-lang.modules" %% "scala-parser-combinators" % Versions.scalaParserCombinators,
    "org.scala-lang.modules" %% "scala-xml" % Versions.scalaXml
  )

  val scalaCheck = Seq(
    "org.scalacheck" %% "scalacheck" % Versions.scalaCheck % "test"
  )

  val scalaTest = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalaTest % "test"
  )

  val scalaz = Seq(
    "org.scalaz" %% "scalaz-core" % Versions.scalaz
  )

  val scodec = Seq(
    "org.scodec" %% "scodec-core" % Versions.scodecCore
  )

  val scopt = Seq(
    "com.github.scopt" %% "scopt" % Versions.scopt
  )

  val shapeless = Seq(
    "com.chuusai" %% "shapeless" % Versions.shapeless
  )

  val slf4j = Seq(
    "org.slf4j" % "slf4j-api" % Versions.slf4j,
    "org.slf4j" % "slf4j-log4j12" % Versions.slf4j,
    "org.slf4j" % "jul-to-slf4j" % Versions.slf4j
  )

  val test = Seq.empty[ModuleID]

  val all =
    test ++ config ++ janino ++ log4j ++ parquetMr ++ scala ++ scodec ++
      scopt ++ scalaCheck ++ scalaTest ++ scalaz ++ shapeless ++ slf4j

  val overrides = Set.empty ++ protobuf ++ scala
}
