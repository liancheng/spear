import Dependencies._

lazy val scraper = (project in file("."))
  .aggregate(core, localExecution, repl)

lazy val core = (project in file("core"))
  .enablePlugins(sbtPlugins: _*)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= typesafeConfig ++ logging ++ scala ++ scopt,
    libraryDependencies ++= Dependencies.testing
  )

lazy val localExecution = (project in file("execution/local"))
  .dependsOn(core % "compile->compile;test->test")
  .enablePlugins(sbtPlugins: _*)
  .settings(commonSettings)

lazy val repl = (project in file("repl"))
  .dependsOn(core % "compile->compile;test->test")
  .dependsOn(localExecution % "compile->compile;test->test")
  .enablePlugins(sbtPlugins: _*)
  .settings(commonSettings ++ runtimeConfSettings)
  .settings(libraryDependencies ++= ammonite)

lazy val examples = (project in file("examples"))
  .dependsOn(core, localExecution)
  .enablePlugins(sbtPlugins: _*)
  .settings(commonSettings ++ runtimeConfSettings)

lazy val sbtPlugins = Seq(
  // For packaging
  JavaAppPackaging,
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
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xlint"),
    javacOptions ++= Seq("-source", "1.7", "-target", "1.7", "-g", "-Xlint:-options")
  )

  val commonTestSettings = Seq(
    fork := false,
    parallelExecution in Test := false,
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

    scalariformSettings ++ Seq(preferences := loadPreferences("scalariform.properties"))
  }

  val taskSettings = Seq(
    // Runs scalastyle before compilation
    compile in Compile <<= compile in Compile dependsOn (scalastyle in Compile toTask ""),
    // Runs scalastyle before running tests
    test in Test <<= test in Test dependsOn (scalastyle in Test toTask "")
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
