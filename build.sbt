// @formatter:off
ThisBuild / scalaVersion     := "2.12.15"
ThisBuild / version          := "1.0.0"
ThisBuild / organization     := "me.rotemfo"
ThisBuild / organizationName := "rotemfo"
// @formatter:on

lazy val root = (project in file("."))
  .settings(
    name := "s3-access-logs",
    Compile / scalacOptions ++= Seq(
      "-target:jvm-1.8",
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xfatal-warnings",
      "-Xfuture",
      "-Xlint",
      "-Yrangepos",
      "-Ywarn-dead-code",
      "-Ywarn-nullary-unit",
      "-Ywarn-unused-import",
      "-Ywarn-unused"),
    libraryDependencies ++= {
      // @formatter:off
      lazy val sparkVersion        = "3.1.2"
      lazy val sparkTestVersion    = "1.1.1"
      lazy val sparkTestingVersion = s"${sparkVersion}_$sparkTestVersion"
      Seq(
        "org.slf4j"                   % "slf4j-api"          % "1.7.32",
        "ch.qos.logback"              % "logback-classic"    % "1.2.7",
        "org.apache.logging.log4j"    % "log4j-to-slf4j"     % "2.14.1",
        "org.apache.spark"           %% "spark-sql"          % sparkVersion        % Provided,
        "org.apache.hadoop"           % "hadoop-aws"         % "3.3.1"             % Provided,
        "com.github.scopt"           %% "scopt"              % "4.0.1",
        // -- test resources
        "org.apache.spark"           %% "spark-hive"         % sparkVersion        % Test,
        "com.holdenkarau"            %% "spark-testing-base" % sparkTestingVersion % Test
      )
      // @formatter:on
    },
    assemblyPackageScala / assembleArtifact := false,
    assemblyMergeStrategy := {
      case "module-info.class" => MergeStrategy.discard
      case x =>
        val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / assemblyJarName := "s3-access-logs-assembly.jar"
  )
