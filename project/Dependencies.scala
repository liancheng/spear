import sbt._

object Dependencies {
  val extraResolvers = Seq(
    Resolver.mavenLocal,
    "Twitter Maven" at "http://maven.twttr.com"
  )

  object Versions {
    val ammonite = "0.6.2"
    val config = "1.2.1"
    val jline = "2.12.1"
    val log4j = "1.2.16"
    val protobuf = "2.5.0"
    val scala = "2.11.8"
    val scalaCheck = "1.12.5"
    val scalaParserCombinators = "1.0.4"
    val scalaXml = "1.0.4"
    val scalaTest = "2.2.5"
    val scopt = "3.4.0"
    val slf4j = "1.6.4"
  }

  val ammonite = Seq(
    "com.lihaoyi" % "ammonite-repl_2.11.8" % Versions.ammonite
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
