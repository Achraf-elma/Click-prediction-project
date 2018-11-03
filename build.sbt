name := "click_prediction"

version := "0.1"

scalaVersion := "2.11.12"
sparkVersion := "2.2.0"
sparkComponents ++= Seq("sql")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0"


