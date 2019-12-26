name := "otusde201911hex7"

version := "1"

scalaVersion := "2.11.12"

//resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4" //% "provided"

libraryDependencies += "org.scala-lang" % "scala-xml" % "2.11.0-M4" //% "provided"

libraryDependencies += "org.apache.logging.log4j" % "log4j" % "2.13.0" //% "provided"

//javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

