organization := "org.sullivan"
name         := "titanic"
spName       := "titanic"
version      := "0.0.1"
sparkVersion := "2.3.0"
scalaVersion := "2.11.8"

sparkComponents += "mllib"

libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.18.0"
