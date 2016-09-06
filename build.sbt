import Dependencies._

lazy val repl = taskKey[Unit]("Runs the Scraper REPL.")

lazy val scraper =
  Project(id = "scraper", base = file("."))
    .aggregate(scraperCore, scraperExamples, scraperLocal, scraperRepl)
    .settings(repl := (run in scraperRepl in Compile toTask "").value)

lazy val scraperCore =
  Project(id = "scraper-core", base = file("scraper-core"))
    .enablePlugins(sbtPlugins: _*)
    .settings(commonSettings)
    .settings(
      libraryDependencies ++= typesafeConfig ++ logging ++ scala ++ scopt,
      libraryDependencies ++= Dependencies.testing
    )

lazy val scraperLocal =
  Project(id = "scraper-local", base = file("scraper-local"))
    .dependsOn(scraperCore % "compile->compile;test->test")
    .enablePlugins(sbtPlugins: _*)
    .settings(commonSettings)

lazy val scraperRepl =
  Project(id = "scraper-repl", base = file("scraper-repl"))
    .dependsOn(scraperCore % "compile->compile;test->test")
    .dependsOn(scraperLocal % "compile->compile;test->test")
    .enablePlugins(sbtPlugins: _*)
    .settings(commonSettings ++ runtimeConfSettings)
    .settings(libraryDependencies ++= ammonite)

lazy val scraperExamples =
  Project(id = "scraper-examples", base = file("scraper-examples"))
    .dependsOn(scraperCore, scraperLocal)
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
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
    scalacOptions ++= Seq("-Ywarn-unused-import", "-Xlint"),
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
