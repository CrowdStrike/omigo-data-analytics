name := "Statistical Trends Analysis in Spark"

version := "0.1"

scalaVersion := "3.2.2"

val sparkVersion = "3.2.0"

// Must turn of parallel execution of tests -- we can only have one Spark
// context active at any given time
// parallelExecution in Test := false

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8") 

// addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-sql" % "3.2.0" % "provided").cross(CrossVersion.for3Use2_13)
)

libraryDependencies ++= Seq(
  "com.facebook.presto" % "presto-jdbc" % "0.156",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.0" 
)

libraryDependencies ++= Seq(
  // "org.apache.parquet" % "parquet-format" % "2.8.0",
  // "org.apache.parquet" % "parquet-hadoop" % "1.12.0-SNAPSHOT",
  // "org.apache.parquet" % "parquet-column" % "1.12.0-SNAPSHOT",
  // "org.apache.parquet" % "parquet-common" % "1.12.0-SNAPSHOT",
  // "org.apache.parquet" % "parquet-encoding" % "1.12.0-SNAPSHOT"
  // "org.scala-lang.modules" %% "scala-parallel-collections" % "0.2.0"
)


// libraryDependencies ++= Seq(
//   "io.spray" %%  "spray-json" % "1.3.5"
// )

// These will change to nexus.csdc.beta soon, there is a ticket out to fix
// the issue which requires the nexus.ec2.beta workaround
resolvers ++= Seq(
  // "CrowdStrike Public" at "https://nexus.ec2.beta:8081/repository/public",
  // "CrowdStrike Releases" at "https://nexus.ec2.beta:8081/repository/releases",
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

// The buildinfo.properties files in the cloud-event_proto and platform_event_proto
// files are causing problems when building an assembly JAR, so we manually work
// around it by defining a merge strategy.
assemblyMergeStrategy in assembly := {
  case PathList("buildinfo.properties") => MergeStrategy.rename
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
