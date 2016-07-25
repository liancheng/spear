import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys.preferences
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import net.virtualvoid.sbt.graph.{Plugin => SbtDependencyGraph}
import sbt.Keys._
import sbt._
import scalariform.formatter.preferences.PreferencesImporterExporter
import scoverage.ScoverageSbtPlugin

lazy val scraper = (project in file("."))
  .aggregate(core, localExecution, repl)
  .settings(commonSettings)

lazy val core = (project in file("core"))
  .enablePlugins(sbtPlugins: _*)
  .settings(commonSettings)
  .settings(libraryDependencies ++= coreDependencies)
  // Explicitly overrides all conflicting transitive dependencies
  .settings(dependencyOverrides ++= Dependencies.overrides)

lazy val localExecution = (project in file("execution/local"))
  .dependsOn(core % "compile->compile;test->test")
  .enablePlugins(sbtPlugins: _*)
  .settings(name := "execution-local")
  .settings(commonSettings)
  .settings(libraryDependencies ++= localExecutionDependencies)
  // Explicitly overrides all conflicting transitive dependencies
  .settings(dependencyOverrides ++= Dependencies.overrides)

lazy val repl = (project in file("repl"))
  .dependsOn(core % "compile->compile;test->test")
  .dependsOn(localExecution % "compile->compile;test->test")
  .enablePlugins(sbtPlugins: _*)
  .settings(commonSettings)
  .settings(libraryDependencies ++= Dependencies.ammonite)
  // Explicitly overrides all conflicting transitive dependencies
  .settings(dependencyOverrides ++= Dependencies.overrides)
  .settings(unmanagedClasspath in Runtime += baseDirectory.value.getParentFile / "conf")

lazy val examples = (project in file("examples"))
  .dependsOn(core, localExecution)
  .enablePlugins(sbtPlugins: _*)
  .settings(commonSettings)
  .settings(unmanagedClasspath in Runtime += baseDirectory.value.getParentFile / "conf")

lazy val coreDependencies = {
  import Dependencies._
  test ++ config ++ log4j ++ scala ++ scopt ++ slf4j
}

lazy val localExecutionDependencies = Dependencies.test

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

lazy val commonSettings = basicSettings ++ generalDependencySettings ++ scalariformSettings

lazy val basicSettings = Seq(
  organization := "scraper",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := Dependencies.Versions.scala,
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
  javacOptions ++= Seq("-source", "1.7", "-target", "1.7", "-g", "-Xlint:-options"),
  fork := false,
  parallelExecution in Test := false,
  // Shows duration and full exception stack trace
  testOptions in Test += Tests.Argument("-oDF")
)

lazy val generalDependencySettings = SbtDependencyGraph.graphSettings ++ Seq(
  // Does not copy managed dependencies into `lib_managed`
  retrieveManaged := false,
  // Enables extra resolvers
  resolvers ++= Dependencies.extraResolvers,
  // Disables auto conflict resolution
  conflictManager := ConflictManager.strict
)

lazy val scalariformSettings = SbtScalariform.scalariformSettings ++ Seq(
  preferences := PreferencesImporterExporter.loadPreferences("scalariform.properties")
)
