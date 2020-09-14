name := "QAL"

version := "1.0"

scalaVersion := "2.11.12"

organization := "TUE"

val algebraVersion = "0.6.0"
val javaEwahVersion = "1.1.4"
val sparkVersion = "2.4.3"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.12"

libraryDependencies += "org.antlr" % "antlr4" % "4.6"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.6"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.6"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.6.6"


libraryDependencies ++= Seq(
  "org.apache.calcite.avatica" % "avatica" % "1.13.0",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-yarn" % sparkVersion,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.typelevel" %% "algebra" % algebraVersion,
  "com.googlecode.javaewah" % "JavaEWAH" % javaEwahVersion,
  "com.twitter" % "algebird-core_2.11" % "0.12.3",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.2",
  "com.github.nscala-time" %% "nscala-time" % "1.8.0",
  "com.sparkjava" % "spark-core" % "2.9.1",
  "com.sparkjava" % "spark-core" % "2.9.1"
)
