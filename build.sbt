ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

ThisBuild / javacOptions ++= Seq("--release", "11")

Test / fork := true

lazy val root = (project in file("."))
  .settings(
    name := "FinAnalysis",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.4",
      "org.apache.spark" %% "spark-sql" % "3.5.4",
      "org.apache.spark" %% "spark-streaming" % "3.5.4"
    )
  )

libraryDependencies += "io.github.cdimascio" % "java-dotenv" % "5.2.2"
libraryDependencies += "com.typesafe" % "config" % "1.4.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % Test