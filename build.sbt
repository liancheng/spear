import Dependencies._

lazy val repl = taskKey[Unit]("Runs the Spear REPL.")

lazy val spear = project
  .in(file("."))
  .aggregate(
    `spear-core`,
    `spear-docs`,
    `spear-examples`,
    `spear-local`,
    `spear-repl`,
    `spear-tree`
  )
  .settings(
    // Creates a SBT task alias "repl" that starts the REPL within an SBT session.
    repl := (run in `spear-repl` in Compile toTask "").value
  )

lazy val `spear-tree` = project
  .enablePlugins(commonPlugins: _*)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= logging ++ scala,
    libraryDependencies ++= Dependencies.testing
  )

lazy val `spear-core` = project
  .dependsOn(`spear-tree` % "compile->compile;test->test")
  .enablePlugins(commonPlugins: _*)
  .settings(commonSettings)
  .settings(libraryDependencies ++= fastparse ++ typesafeConfig)

lazy val `spear-local` = project
  .dependsOn(`spear-core` % "compile->compile;test->test")
  .enablePlugins(commonPlugins: _*)
  .settings(commonSettings)

lazy val `spear-repl` = project
  .dependsOn(`spear-core` % "compile->compile;test->test")
  .dependsOn(`spear-local` % "compile->compile;test->test;compile->test")
  .enablePlugins(commonPlugins :+ JavaAppPackaging: _*)
  .settings(commonSettings ++ runtimeConfSettings ++ javaPackagingSettings)
  .settings(libraryDependencies ++= ammonite)

lazy val `spear-examples` = project
  .dependsOn(`spear-core`, `spear-local`)
  .enablePlugins(commonPlugins :+ JavaAppPackaging: _*)
  .settings(commonSettings ++ runtimeConfSettings ++ javaPackagingSettings)

lazy val `spear-docs` = project
  .dependsOn(`spear-core`, `spear-local`)
  .enablePlugins(commonPlugins :+ SphinxPlugin: _*)
  .settings(commonSettings ++ runtimeConfSettings)

lazy val javaPackagingSettings = {
  import NativePackagerHelper._

  Seq(
    // Adds the "conf" directory into the package.
    mappings in Universal ++= directory(baseDirectory(_.getParentFile / "conf").value),
    // Adds the "conf" directory to runtime classpath (relative to "$app_home/../lib").
    scriptClasspath += "../conf"
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
    organization := "spear",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := Versions.scala,
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
    scalacOptions ++= Seq("-Ywarn-unused-import", "-Xlint"),
    javacOptions ++= Seq("-source", "1.7", "-target", "1.7", "-g", "-Xlint:-options")
  )

  val commonTestSettings = Seq(
    // Disables parallel test execution to ensure logging order.
    parallelExecution in Test := false,
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
