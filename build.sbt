name := "paratd"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies += "org.nd4j" % "nd4j-native-platform" % "0.8.0"
libraryDependencies += "org.nd4j" %% "nd4s" % "0.8.0"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.1"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25"
