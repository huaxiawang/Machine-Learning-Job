name := "job_server"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

libraryDependencies ++= Seq(
  "spark.jobserver" %% "job-server-api" % "0.5.1" % "provided",
  "spark.jobserver" %% "job-server-extras" % "0.5.1" % "provided",
  "org.apache.spark" % "spark-core_2.10" % "1.3.1" % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % "1.3.1" % "provided",
  "org.apache.spark" % "spark-hive_2.10" % "1.3.1" % "provided"
)

crossPaths := false

autoScalaLibrary := false