lazy val modules: Seq[ProjectReference] = Seq(
  `spear-core`,
  `spear-docs`,
  `spear-examples`,
  `spear-local`,
  `spear-repl`,
  `spear-trees`,
  `spear-utils`
)

lazy val spear = {
  lazy val repl = taskKey[Unit]("Runs the Spear REPL.")

  Project(id = "spear", base = file("."))
    .aggregate(modules: _*)
    // Creates a SBT task alias "repl" that starts the REPL within an SBT session.
    .settings(repl := (run in `spear-repl` in Compile toTask "").value)
}

def spearModule(name: String): Project =
  Project(id = name, base = file(name))
    .enablePlugins(commonPlugins: _*)
    .settings(commonSettings)

lazy val `spear-utils` = spearModule("spear-utils")
  .settings(libraryDependencies ++= Dependencies.logging)
  .settings(libraryDependencies ++= Dependencies.scala)
  .settings(libraryDependencies ++= Dependencies.testing)

lazy val `spear-trees` = spearModule("spear-trees")
  .dependsOn(`spear-utils` % "compile->compile;test->test")

lazy val `spear-core` = spearModule("spear-core")
  .dependsOn(`spear-trees` % "compile->compile;test->test")
  .settings(libraryDependencies ++= Dependencies.fastparse)
  .settings(libraryDependencies ++= Dependencies.typesafeConfig)

lazy val `spear-local` = spearModule("spear-local")
  .dependsOn(`spear-core` % "compile->compile;test->test")

lazy val `spear-repl` = spearModule("spear-repl")
  .dependsOn(`spear-core` % "compile->compile;test->test")
  .dependsOn(`spear-local` % "compile->compile;test->test;compile->test")
  .enablePlugins(JavaAppPackaging)
  .settings(runtimeConfSettings)
  .settings(javaPackagingSettings)
  .settings(libraryDependencies ++= Dependencies.ammonite)
  .settings(libraryDependencies ++= Dependencies.scopt)

lazy val `spear-examples` = spearModule("spear-examples")
  .dependsOn(`spear-core`, `spear-local`)
  .enablePlugins(JavaAppPackaging)
  .settings(runtimeConfSettings)
  .settings(javaPackagingSettings)

lazy val `spear-docs` = spearModule("spear-docs")
  .dependsOn(`spear-core`, `spear-local`)
  .enablePlugins(SphinxPlugin)

lazy val javaPackagingSettings = {
  import NativePackagerHelper.directory

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
  val buildSettings = Seq(
    organization := "spear",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := Dependencies.Versions.scala,
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
      // Avoids copying managed dependencies into `lib_managed`
      retrieveManaged := false,
      // Enables extra resolvers
      resolvers ++= Dependencies.extraResolvers,
      // Disables auto conflict resolution
      conflictManager := ConflictManager.strict,
      // Explicitly overrides all conflicting transitive dependencies
      dependencyOverrides ++= Dependencies.overrides
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
    buildSettings,
    commonTestSettings,
    commonDependencySettings,
    scalariformPluginSettings,
    taskSettings
  ).flatten
}

lazy val runtimeConfSettings = Seq(
  unmanagedClasspath in Runtime += baseDirectory { _.getParentFile / "conf" }.value
)
