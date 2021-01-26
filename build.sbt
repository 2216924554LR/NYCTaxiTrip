name := "NYCTaxiTrip"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark"%"spark-streaming_2.11"%"2.1.1"
libraryDependencies += "junit"%"junit"%"4.11"
libraryDependencies += "org.apache.spark"%"spark-sql_2.11"%"2.1.1"
libraryDependencies += "org.apache.spark"%"spark-core_2.11"%"2.1.1"
libraryDependencies += "org.json4s"%"json4s-native_2.11"%s"3.5.1"
libraryDependencies += "org.json4s"%"json4s-jackson_2.11"%s"3.5.1"
libraryDependencies += "com.esri.geometry"%"esri-geometry-api"%"2.2.2"
dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
  )
}