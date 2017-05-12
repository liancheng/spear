import Dependencies._
import NativePackagerHelper._

lazy val repl = taskKey[Unit]("Runs the Scraper REPL.")

lazy val scraper = project
  .in(file("."))
  .aggregate(`scraper-core`, `scraper-examples`, `scraper-local`, `scraper-repl`)
  .settings(
    // Creates a SBT task alias "repl" that starts the REPL within an SBT session.
    repl := (run in `scraper-repl` in Compile toTask "").value
  )

lazy val `scraper-core` = project
  .enablePlugins(commonPlugins: _*)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= fastparse ++ typesafeConfig ++ logging ++ scala,
    libraryDependencies ++= Dependencies.testing
  )

lazy val `scraper-local` = project
  .dependsOn(`scraper-core` % "compile->compile;test->test")
  .enablePlugins(commonPlugins: _*)
  .settings(commonSettings)

lazy val `scraper-repl` = project
  .dependsOn(`scraper-core` % "compile->compile;test->test")
  .dependsOn(`scraper-local` % "compile->compile;test->test;compile->test")
  .enablePlugins(commonPlugins :+ JavaAppPackaging: _*)
  .settings(commonSettings ++ runtimeConfSettings ++ javaPackagingSettings)
  .settings(libraryDependencies ++= ammonite)

lazy val `scraper-examples` = project
  .dependsOn(`scraper-core`, `scraper-local`)
  .enablePlugins(commonPlugins: _*)
  .settings(commonSettings ++ runtimeConfSettings)

lazy val javaPackagingSettings = {
  val confDirectory = baseDirectory(_.getParentFile / "conf")

  Seq(
    // Adds the "conf" directory into the package.
    mappings in Universal ++= directory(confDirectory.value),
    // Adds the "conf" directory to runtime classpath.
    scriptClasspath in Universal += confDirectory.value.toString
  )
}

lazy val commonPlugins = Seq(
  // For Scala code formatting
  SbtScalariform,
  // For Scala test coverage reporting
  ScoverageSbtPlugin
)

lazy val commonSettings = {
  val basicSettings = Seq(
    organization := "scraper",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := Versions.scala,
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
    scalacOptions ++= Seq("-Ywarn-unused-import", "-Xlint"),
    javacOptions ++= Seq("-source", "1.7", "-target", "1.7", "-g", "-Xlint:-options")
  )

  val commonTestSettings = Seq(
    // Does not fork a new JVM process to run the tests.
    fork := false,
    // Shows duration and full exception stack trace
    testOptions in Test += Tests.Argument("-oDF")
  )

  val commonDependencySettings = {
    import net.virtualvoid.sbt.graph.Plugin.graphSettings

    graphSettings ++ Seq(
      // Does not copy managed dependencies into `lib_managed`
      retrieveManaged := false,
      // Enables extra resolvers
      resolvers ++= extraResolvers,
      // Disables auto conflict resolution
      conflictManager := ConflictManager.strict,
      // Explicitly overrides all conflicting transitive dependencies
      dependencyOverrides ++= overrides
    )
  }

  val scalariformPluginSettings = {
    import com.typesafe.sbt.SbtScalariform.scalariformSettings
    import com.typesafe.sbt.SbtScalariform.ScalariformKeys.preferences
    import scalariform.formatter.preferences.PreferencesImporterExporter.loadPreferences

    scalariformSettings ++ Seq(
      preferences := loadPreferences("scalariform.properties")
    )
  }

  val taskSettings = Seq(
    // Runs scalastyle before compilation
    compile in Compile := (compile in Compile dependsOn (scalastyle in Compile toTask "")).value,
    // Runs scalastyle before running tests
    test in Test := (test in Test dependsOn (scalastyle in Test toTask "")).value
  )

  Seq(
    basicSettings,
    commonTestSettings,
    commonDependencySettings,
    scalariformPluginSettings,
    taskSettings
  ).flatten
}

lazy val runtimeConfSettings = Seq(
  unmanagedClasspath in Runtime += baseDirectory(_.getParentFile / "conf").value
)
