name := "Assignment"
version := "1.0"
scalaVersion := "2.10.4"
mainClass := Some("main")

libraryDependencies += "org.apache.commons" % "commons-csv" % "1.2"
libraryDependencies += "net.sf.opencsv" % "opencsv" % "2.3"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"
libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaVersion.value
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.2"
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "0.1"