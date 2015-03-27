import sbtrelease.ReleaseStateTransformations._
import scalariform.formatter.preferences._
import scala.util.Properties._
import xerial.sbt.Sonatype._
import sbtrelease._

import com.typesafe.sbt.pgp.PgpKeys

// Project Info //

name := "aerospikez"

description := "Aerospike v3 Scala Client"

organization := "com.github.otrimegistro"

homepage := Some(url("http://github.com/otrimegistro/aerospikez"))

startYear := Some(2014)

licenses := Seq(
  ("MIT License", url("http://raw.github.com/otrimegistro/aerospikez/master/LICENSE"))
)

scmInfo := Some(
  ScmInfo(
    url("https://github.com/otrimegistro/aerospikez"),
    "scm:git:https://github.com/otrimegistro/aerospikez.git",
    Some("scm:git:git@github.com:otrimegistro/aerospikez.git")
  )
)

// Dependencies //

resolvers ++= Seq(
  "Scalaz Bintray Repo"   at "http://dl.bintray.com/scalaz/releases",
  "Typesafe Repository"   at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/"
)

libraryDependencies ++= {
  val scalazV = "7.1.0"
  Seq(
    "com.aerospike"     % "aerospike-client"    % "3.0.33",
    "org.scalaz"        %% "scalaz-core"        % scalazV,
    "org.scalaz"        %% "scalaz-concurrent"  % scalazV,
    "org.scalaz.stream" %% "scalaz-stream"      % "0.5a",
    "com.typesafe"      %  "config"             % "1.2.1",
    "org.gnu"           %  "gnu-crypto"         % "2.0.1",
    "org.luaj"          %  "luaj-jse"           % "3.0",
    "org.specs2"        %% "specs2"             % "2.4" % "test"
  )
}

// Settings //

offline := true

scalaVersion := "2.11.4"

compileOrder := CompileOrder.JavaThenScala

incOptions := incOptions.value.withNameHashing(true)

scalacOptions ++= Seq(
  "-feature",
  "-Xfuture",
  "-optimise",
  "-Xmigration",
  "-deprecation",
  "-language:higherKinds",
  "-language:implicitConversions"
)

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(RewriteArrowSymbols, true)
  .setPreference(AlignParameters, true)

// Publishing //

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

Seq("SONATYPE_USER", "SONATYPE_PASS") map envOrNone match {
  case Seq(Some(user), Some(pass)) =>
    credentials += Credentials(
      "Sonatype Nexus Repository Manager",
      "oss.sonatype.org", user, pass)
  case _ =>
    credentials ~= identity
}

sonatypeSettings

releaseSettings

def releaseStepCross[A](key: TaskKey[A]) = ReleaseStep(
  action = state => Project.extract(state).runTask(key, state)._1,
  enableCrossBuild = true
)

ReleaseKeys.releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCross(PgpKeys.publishSigned),
  setNextVersion,
  commitNextVersion,
  releaseStepCross(SonatypeKeys.sonatypeReleaseAll),
  pushChanges
)

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <developers>
    <developer>
      <id>otrimegistro</id>
      <name>Omar González</name>
      <url>https://github.com/otrimegistro</url>
    </developer>
  </developers>
)
