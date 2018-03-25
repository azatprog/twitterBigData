name := "BigData"

version := "0.1"

scalaVersion := "2.11.11"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

javaOptions ++= Seq(
  "-Xdebug",
  "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"
)
