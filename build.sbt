lazy val repl = taskKey[Unit]("Runs the Spear REPL.")

lazy val spear = project
  .in(file("."))
  // Creates a SBT task alias "repl" that starts the REPL within an SBT session.
  .settings(repl := (run in `spear-repl` in Compile toTask "").value)
  .settings(commonSettings)
  .aggregate(allSubprojects: _*)

lazy val allSubprojects: Seq[ProjectReference] = Seq(
  `spear-core`,
  `spear-docs`,
  `spear-examples`,
  `spear-local`,
  `spear-repl`,
  `spear-trees`,
  `spear-utils`
)

lazy val `spear-utils` = project
  .enablePlugins(commonPlugins: _*)
  .settings(libraryDependencies ++= Dependencies.logging ++ Dependencies.scala)
  .settings(libraryDependencies ++= Dependencies.testing)

lazy val `spear-trees` = project
  .dependsOn(`spear-utils` % "compile->compile;test->test")
  .enablePlugins(commonPlugins: _*)

lazy val `spear-core` = project
  .dependsOn(`spear-trees` % "compile->compile;test->test")
  .enablePlugins(commonPlugins: _*)
  .settings(libraryDependencies ++= Dependencies.fastparse ++ Dependencies.typesafeConfig)

lazy val `spear-local` = project
  .dependsOn(`spear-core` % "compile->compile;test->test")
  .enablePlugins(commonPlugins: _*)

lazy val `spear-repl` = project
  .dependsOn(`spear-core` % "compile->compile;test->test")
  .dependsOn(`spear-local` % "compile->compile;test->test;compile->test")
  .enablePlugins(commonPlugins :+ JavaAppPackaging: _*)
  .settings(runtimeConfSettings ++ javaPackagingSettings)
  .settings(libraryDependencies ++= Dependencies.ammonite ++ Dependencies.scopt)

lazy val `spear-examples` = project
  .dependsOn(`spear-core`, `spear-local`)
  .enablePlugins(commonPlugins :+ JavaAppPackaging: _*)
  .settings(runtimeConfSettings ++ javaPackagingSettings)

lazy val `spear-docs` = project
  .dependsOn(`spear-core`, `spear-local`)
  .enablePlugins(commonPlugins :+ SphinxPlugin: _*)
  .settings(runtimeConfSettings)

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
  SbtScalariform
)

lazy val commonSettings = {
  val basicSettings = Seq(
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

  val commonDependencySettings = Seq(
    // Does not copy managed dependencies into `lib_managed`
    retrieveManaged := false,
    // Enables extra resolvers
    resolvers ++= Dependencies.extraResolvers,
    // Disables auto conflict resolution
    conflictManager := ConflictManager.strict,
    // Explicitly overrides all conflicting transitive dependencies
    dependencyOverrides ++= Dependencies.overrides
  )

  val scalariformSettings = Seq {
    import com.typesafe.sbt.SbtScalariform.ScalariformKeys
    import scalariform.formatter.preferences

    ScalariformKeys.preferences := ScalariformKeys.preferences.value
      .setPreference(preferences.AlignSingleLineCaseStatements, true)
      .setPreference(preferences.AlignSingleLineCaseStatements.MaxArrowIndent, 40)
      .setPreference(preferences.DanglingCloseParenthesis, preferences.Preserve)
      .setPreference(preferences.NewlineAtEndOfFile, true)
      .setPreference(preferences.PreserveSpaceBeforeArguments, true)
      .setPreference(preferences.SpacesAroundMultiImports, false)
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
    scalariformSettings,
    taskSettings
  ).flatten
}

lazy val runtimeConfSettings = Seq(
  unmanagedClasspath in Runtime += baseDirectory { _.getParentFile / "conf" }.value
)
