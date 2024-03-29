
name := "QAL"

version := "1.0"

scalaVersion := "2.11.12"

organization := "TUE"
unmanagedJars in Compile += file("lib/avatica-1.13.0.jar")
val algebraVersion = "0.6.0"
val javaEwahVersion = "1.1.4"
val sparkVersion = "2.4.3"

unmanagedJars in Compile += file("~/avatica-1.13.0.jar")

libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.12"

libraryDependencies += "org.antlr" % "antlr4" % "4.6"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.6"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.6"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.6.6"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-yarn" % sparkVersion,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion,
  "co.theasi" %% "plotly" % "0.2.0",
  "com.github.wookietreiber" %% "scala-chart" % "0.5.1",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.typelevel" %% "algebra" % algebraVersion,
  "com.googlecode.javaewah" % "JavaEWAH" % javaEwahVersion,
  "com.twitter" % "algebird-core_2.11" % "0.12.3",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.2",
  "com.github.nscala-time" %% "nscala-time" % "1.8.0",
  "com.sparkjava" % "spark-core" % "2.9.1"
)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := "SDL_QAL_API.jar"