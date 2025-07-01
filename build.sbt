import com.typesafe.tools.mima.plugin.MimaKeys.mimaPreviousArtifacts
import com.typesafe.tools.mima.plugin.MimaKeys.mimaReportSignatureProblems
import sbt.Keys.parallelExecution
import com.geirsson.CiReleasePlugin

GlobalScope / parallelExecution := false
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

inThisBuild(
  Seq(
    organization := "com.lightbend.akka",
    organizationName := "Lightbend Inc.",
    homepage := Some(url("https://doc.akka.io/libraries/akka-persistence-dynamodb/current")),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/akka/akka-persistence-dynamodb"),
        "https://github.com/akka/akka-persistence-dynamodb.git")),
    startYear := Some(2024),
    developers += Developer(
      "contributors",
      "Contributors",
      "",
      url("https://github.com/akka/akka-persistence-dynamodb/graphs/contributors")),
    releaseNotesURL := (
      if (isSnapshot.value) None
      else Some(url(s"https://github.com/akka/akka-persistence-dynamodb/releases/tag/v${version.value}"))
    ),
    licenses := {
      val tagOrBranch =
        if (isSnapshot.value) "main"
        else "v" + version.value
      Seq(("BUSL-1.1", url(s"https://github.com/akka/akka-persistence-dynamodb/blob/${tagOrBranch}/LICENSE")))
    },
    description := "An Akka Persistence plugin backed by Amazon DynamoDB",
    // append -SNAPSHOT to version when isSnapshot
    dynverSonatypeSnapshots := true,
    resolvers += "Akka library repository".at("https://repo.akka.io/maven/github_actions"),
    resolvers ++=
      (if (Dependencies.AkkaVersion.endsWith("-SNAPSHOT"))
         Seq("Akka library snapshot repository".at("https://repo.akka.io/snapshots/github_actions"))
       else Seq.empty)))

val defaultScalacOptions = Seq("-release", "11")

def common: Seq[Setting[_]] =
  Seq(
    crossScalaVersions := Dependencies.ScalaVersions,
    scalaVersion := Dependencies.Scala213,
    crossVersion := CrossVersion.binary,
    scalafmtOnCompile := true,
    // Setting javac options in common allows IntelliJ IDEA to import them automatically
    Compile / javacOptions ++= Seq("-encoding", "UTF-8", "--release", "11"),
    Compile / javacOptions ++= Seq("-Werror", "-Xlint:deprecation", "-Xlint:unchecked"),
    Compile / scalacOptions ++= defaultScalacOptions,
    Compile / scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 13)) => Seq("-Xfatal-warnings", "-Xlint", "-unchecked", "-deprecation")
      case _             => Seq("-Xfatal-warnings")
    }),
    Compile / console / scalacOptions := defaultScalacOptions,
    Test / console / scalacOptions := defaultScalacOptions,
    Compile / doc / scalacOptions := defaultScalacOptions ++ Seq(
      "-doc-title",
      "Akka Persistence DynamoDB",
      "-doc-version",
      version.value) ++ {
      // make use of https://github.com/scala/scala/pull/8663
      if (scalaBinaryVersion.value.startsWith("3")) {
        Seq(
          s"-external-mappings:https://docs.oracle.com/en/java/javase/${Dependencies.JavaDocLinkVersion}/docs/api/java.base/")
      } else
        Seq(
          "-jdk-api-doc-base",
          s"https://docs.oracle.com/en/java/javase/${Dependencies.JavaDocLinkVersion}/docs/api/java.base/")
    },
    Compile / doc / autoAPIMappings := true,
    headerLicense := Some(HeaderLicense.Custom("""Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>""")),
    Test / logBuffered := false,
    Test / parallelExecution := false,
    // show full stack traces and test case durations
    Test / testOptions += Tests.Argument("-oDF"),
    // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
    // -a Show stack traces and exception class name for AssertionErrors.
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
    Test / fork := true, // some non-heap memory is leaking
    Test / javaOptions ++= {
      import scala.collection.JavaConverters._
      // include all passed -Dakka. properties to the javaOptions for forked tests
      // useful to switch DB dialects for example
      val akkaProperties = System.getProperties.stringPropertyNames.asScala.toList.collect {
        case key: String if key.startsWith("akka.") || key.startsWith("conf") =>
          "-D" + key + "=" + System.getProperty(key)
      }
      "-Xms1G" :: "-Xmx1G" :: "-XX:MaxDirectMemorySize=256M" :: akkaProperties
    },
    projectInfoVersion := (if (isSnapshot.value) "snapshot" else version.value),
    Global / excludeLintKeys += projectInfoVersion,
    Global / excludeLintKeys += mimaReportSignatureProblems,
    Global / excludeLintKeys += mimaPreviousArtifacts,
    Global / excludeLintKeys += docs / previewSite / previewPath,
    mimaReportSignatureProblems := true,
    mimaPreviousArtifacts := Set.empty
    // FIXME enable after first official release
    // Set(
    //  organization.value %% moduleName.value % previousStableVersion.value
    //    .getOrElse(throw new Error("Unable to determine previous version")))
  )

lazy val dontPublish = Seq(publish / skip := true, Compile / publishArtifact := false)

lazy val root = (project in file("."))
  .settings(common)
  .settings(dontPublish)
  .settings(
    name := "akka-persistence-dynamodb-root",
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))))
  .enablePlugins(ScalaUnidocPlugin)
  .disablePlugins(SitePlugin, MimaPlugin, CiReleasePlugin)
  .aggregate(core, docs)

def suffixFileFilter(suffix: String): FileFilter = new SimpleFileFilter(f => f.getAbsolutePath.endsWith(suffix))

lazy val core = (project in file("core"))
  .settings(common)
  .settings(name := "akka-persistence-dynamodb", libraryDependencies ++= Dependencies.core)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(CiReleasePlugin)

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(AkkaParadoxPlugin, ParadoxSitePlugin, PreprocessPlugin, PublishRsyncPlugin)
  .disablePlugins(MimaPlugin, CiReleasePlugin)
  .dependsOn(core % "compile;test->test")
  .settings(common)
  .settings(dontPublish)
  .settings(
    name := "Akka Persistence plugin for Amazon DynamoDB",
    libraryDependencies ++= (Dependencies.TestDeps.cloudwatchMetricPublisher +: Dependencies.docs),
    makeSite := makeSite.dependsOn(LocalRootProject / ScalaUnidoc / doc).value,
    previewPath := (Paradox / siteSubdirName).value,
    Preprocess / siteSubdirName := s"api/akka-persistence-dynamodb/${projectInfoVersion.value}",
    Preprocess / sourceDirectory := (LocalRootProject / ScalaUnidoc / unidoc / target).value,
    Paradox / siteSubdirName := s"libraries/akka-persistence-dynamodb/${projectInfoVersion.value}",
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    Compile / paradoxProperties ++= Map(
      "project.url" -> "https://doc.akka.io/libraries/akka-persistence-dynamodb/current/",
      "canonical.base_url" -> "https://doc.akka.io/libraries/akka-persistence-dynamodb/current",
      "akka.version" -> Dependencies.AkkaVersion,
      "scala.version" -> scalaVersion.value,
      "scala.binary.version" -> scalaBinaryVersion.value,
      "extref.akka-core.base_url" -> s"https://doc.akka.io/libraries/akka-core/${Dependencies.AkkaVersionInDocs}/%s",
      "extref.akka-projection.base_url" -> s"https://doc.akka.io/libraries/akka-projection/${Dependencies.AkkaProjectionVersionInDocs}/%s",
      "extref.java-docs.base_url" -> "https://docs.oracle.com/en/java/javase/11/%s",
      "scaladoc.com.typesafe.config.base_url" -> s"https://lightbend.github.io/config/latest/api/",
      "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/current/",
      "scaladoc.akka.persistence.dynamodb.base_url" -> s"/${(Preprocess / siteSubdirName).value}/",
      "scaladoc.akka.projection.base_url" -> s"https://doc.akka.io/api/akka-projection/${Dependencies.AkkaProjectionVersionInDocs}/",
      "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka-core/${Dependencies.AkkaVersionInDocs}/",
      "javadoc.akka.persistence.dynamodb.base_url" -> "", // no Javadoc is published
      "javadoc.akka.base_url" -> s"https://doc.akka.io/japi/akka-core/${Dependencies.AkkaVersionInDocs}/"),
    ApidocPlugin.autoImport.apidocRootPackage := "akka",
    apidocRootPackage := "akka",
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifacts += makeSite.value -> "www/",
    publishRsyncHost := "akkarepo@gustav.akka.io")

val isJdk11orHigher: Boolean = {
  val result = VersionNumber(sys.props("java.specification.version")).matchesSemVer(SemanticSelector(">=11"))
  if (!result)
    throw new IllegalArgumentException("JDK 11 or higher is required")
  result
}
