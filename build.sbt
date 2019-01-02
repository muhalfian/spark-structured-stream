import sbt._
import Keys._

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

// lazy val root = Project("root", file(".")) dependsOn(sparkProject)
// lazy val sparkProject = RootProject(uri("git://github.com/muhalfian/custom-spark.git"))


// // ======================== SPARK ==================================
//
// // addSbtPlugin("com.etsy" % "sbt-checkstyle-plugin" % "3.1.1")
// libraryDependencies += "com.etsy" % "sbt-checkstyle-plugin" % "3.1.1"
//
// // sbt-checkstyle-plugin uses an old version of checkstyle. Match it to Maven's.
// libraryDependencies += "com.puppycrawl.tools" % "checkstyle" % "8.14"
//
// // checkstyle uses guava 23.0.
// libraryDependencies += "com.google.guava" % "guava" % "23.0"
//
// // // need to make changes to uptake sbt 1.0 support in "com.eed3si9n" % "sbt-assembly" % "1.14.5"
// // addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2")
// libraryDependencies += "com.eed3si9n" % "sbt-assembly" % "0.11.2"
// //
// // addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "5.2.4")
// libraryDependencies += "com.typesafe.sbteclipse" % "sbteclipse-plugin" % "5.2.4"
// //
// // addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")
// libraryDependencies += "net.virtual-void" % "sbt-dependency-graph" % "0.9.2"
// //
// // addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
// libraryDependencies += "org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0"
// //
// // addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.3.0")
// libraryDependencies += "com.typesafe" % "sbt-mima-plugin" % "0.3.0"
//
// // sbt 1.0.0 support: https://github.com/AlpineNow/junit_xml_listener/issues/6
// // addSbtPlugin("com.alpinenow" % "junit_xml_listener" % "0.5.1")
// libraryDependencies += "com.alpinenow" % "junit_xml_listener" % "0.5.1"
//
// // // need to make changes to uptake sbt 1.0 support in "com.eed3si9n" % "sbt-unidoc" % "0.4.1"
// // addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.2")
// libraryDependencies += "com.eed3si9n" % "sbt-unidoc" % "0.4.2"
// //
// // // need to make changes to uptake sbt 1.0 support in "com.cavorite" % "sbt-avro-1-7" % "1.1.2"
// // addSbtPlugin("com.cavorite" % "sbt-avro-1-8" % "1.1.5")
// libraryDependencies += "com.cavorite" % "sbt-avro-1-8" % "1.1.5"
// //
// // addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")
// libraryDependencies += "io.spray" % "sbt-revolver" % "0.9.1"
//
// libraryDependencies += "org.ow2.asm"  % "asm" % "7.0"
//
// libraryDependencies += "org.ow2.asm"  % "asm-commons" % "7.0"
//
// // // sbt 1.0.0 support: https://github.com/ihji/sbt-antlr4/issues/14
// // addSbtPlugin("com.simplytyped" % "sbt-antlr4" % "0.8.1")
// libraryDependencies += "com.simplytyped" % "sbt-antlr4" % "0.8.1"
//
// // Spark uses a custom fork of the sbt-pom-reader plugin which contains a patch to fix issues
// // related to test-jar dependencies (https://github.com/sbt/sbt-pom-reader/pull/14). The source for
// // this fork is published at https://github.com/JoshRosen/sbt-pom-reader/tree/v1.0.0-spark
// // and corresponds to commit b160317fcb0b9d1009635a7c5aa05d0f3be61936 in that repository.
// // In the long run, we should try to merge our patch upstream and switch to an upstream version of
// // the plugin; this is tracked at SPARK-14401.
//
// // // addSbtPlugin("org.spark-project" % "sbt-pom-reader" % "1.0.0-spark")
// // addSbtPlugin("com.typesafe.sbt" % "sbt-pom-reader" % "2.1.0")
// libraryDependencies += "com.typesafe.sbt" % "sbt-pom-reader" % "2.1.0"
//
// // =======================================================

assemblyMergeStrategy in assembly := {
  {
    case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}

// import sbtassembly.AssemblyUtils._
// import sbtassembly.Plugin._
// import AssemblyKeys._
//
//
// mergeStrategy in assembly := {
//   {
//     case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
//     case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//     case x => MergeStrategy.first
//   }
// }
