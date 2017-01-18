import sbt._

object Dependencies {
  val extraResolvers = Seq(
    Resolver.mavenLocal,
    "Twitter Maven" at "http://maven.twttr.com"
  )

  object Versions {
    val ammonite = "0.7.8"
    val config = "1.2.1"
    val fastparse = "0.4.1"
    val jline = "2.12.1"
    val log4j = "1.2.16"
    val mockito = "2.1.0-beta.120"
    val protobuf = "2.5.0"
    val scala = "2.11.8"
    val scalaCheck = "1.13.4"
    val scalaCheckShapeless = "1.1.4"
    val scalaTest = "3.0.1"
    val scalaXml = "1.0.4"
    val slf4j = "1.6.4"
    val sourcecode = "0.1.2"
  }

  val ammonite = Seq(
    "com.lihaoyi" % "ammonite_2.11.8" % Versions.ammonite
  )

  val fastparse = Seq(
    "com.lihaoyi" %% "fastparse" % Versions.fastparse
  )

  val jline = Seq(
    "jline" % "jline" % Versions.jline
  )

  val log4j = Seq(
    "log4j" % "log4j" % Versions.log4j
  )

  val mockito = Seq(
    "org.mockito" % "mockito-core" % Versions.mockito
  )

  val protobuf = Seq(
    "com.google.protobuf" % "protobuf-java" % Versions.protobuf
  )

  val scala = Seq(
    "org.scala-lang" % "scala-library" % Versions.scala,
    "org.scala-lang" % "scala-reflect" % Versions.scala,
    "org.scala-lang.modules" %% "scala-xml" % Versions.scalaXml
  )

  val scalaCheck = Seq(
    "org.scalacheck" %% "scalacheck" % Versions.scalaCheck,
    "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % Versions.scalaCheckShapeless
  )

  val scalaTest = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalaTest
  )

  val slf4j = Seq(
    "org.slf4j" % "slf4j-api" % Versions.slf4j,
    "org.slf4j" % "slf4j-log4j12" % Versions.slf4j,
    "org.slf4j" % "jul-to-slf4j" % Versions.slf4j
  )

  val sourcecode = Seq(
    "com.lihaoyi" %% "sourcecode" % Versions.sourcecode
  )

  val testing: Seq[ModuleID] = mockito ++ scalaCheck ++ scalaTest map { _ % "test" }

  val typesafeConfig = Seq(
    "com.typesafe" % "config" % Versions.config
  )

  val logging: Seq[ModuleID] = log4j ++ slf4j

  val overrides: Set[ModuleID] = (jline ++ protobuf ++ scala ++ sourcecode).toSet
}
