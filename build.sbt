ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "network-fault-bigdata",
    idePackagePrefix := Some("edu.cose.seu")
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % "3.2.1"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.30"
libraryDependencies += "com.typesafe" % "config" % "1.4.2"
libraryDependencies += "org.junit.jupiter" % "junit-jupiter-api" % "5.8.2" % Test




