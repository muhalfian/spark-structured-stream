name := "Brondo"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.1"

libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.1"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.1"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.0.0"

libraryDependencies += "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.2.2"