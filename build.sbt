name := "Prayuga-Streaming"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.0"

libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.4.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.0"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.0.0"

libraryDependencies += "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.4.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.4.0"

libraryDependencies += "com.andylibrian.jsastrawi" % "jsastrawi" % "0.1"

libraryDependencies += "com.typesafe" % "config" % "1.3.2"

// libraryDependencies += "org.apache.lucene" % "lucene-analyzers-common" % "7.4.0"

assemblyMergeStrategy in assembly := {
  {
    case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}
