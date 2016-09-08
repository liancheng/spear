import sbt._

object Dependencies {
  val extraResolvers = Seq(
    Resolver.mavenLocal,
    "Twitter Maven" at "http://maven.twttr.com"
  )

  object Versions {
    val ammonite = "0.7.6"
    val config = "1.2.1"
    val jline = "2.12.1"
    val log4j = "1.2.16"
    val mockito = "2.1.0-beta.120"
    val protobuf = "2.5.0"
    val scala = "2.11.8"
    val scalaCheck = "1.12.5"
    val scalaParserCombinators = "1.0.4"
    val scalaTest = "2.2.5"
    val scalaXml = "1.0.4"
    val scopt = "3.4.0"
    val slf4j = "1.6.4"
  }

  val ammonite = Seq(
    "com.lihaoyi" % "ammonite" % Versions.ammonite cross CrossVersion.full
  )

  val typesafeConfig = Seq(
    "com.typesafe" % "config" % Versions.config
  )

  val jline = Seq(
    "jline" % "jline" % Versions.jline
  )

  val log4j = Seq(
    "log4j" % "log4j" % Versions.log4j
  )

  val mockito = Seq(
    "org.mockito" % "mockito-core" % Versions.mockito % "test"
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

  val testing = mockito ++ scalaCheck ++ scalaTest

  val logging = log4j ++ slf4j

  val overrides = (jline ++ protobuf ++ scala).toSet
}
