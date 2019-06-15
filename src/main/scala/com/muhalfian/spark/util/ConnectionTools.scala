package com.muhalfian.spark.util

import org.mongodb.scala._
import org.mongodb.scala.model.Updates._

object ConnectionTools {
  // // To directly connect to the default server localhost on port 27017
  // val mongoClient: MongoClient = MongoClient()

  // Use a Connection String
  val mongoClient: MongoClient = MongoClient(PropertiesLoader.mongoUrl)

  // // or provide custom MongoClientSettings
  // val clusterSettings: ClusterSettings = ClusterSettings.builder().hosts(List(new ServerAddress("localhost")).asJava).build()
  // val settings: MongoClientSettings = MongoClientSettings.builder().clusterSettings(clusterSettings).build()
  // val mongoClient: MongoClient = MongoClient(settings)

  val prayugaDb: MongoDatabase = mongoClient.getDatabase(PropertiesLoader.mongoDb)
  val masterClusterDb: MongoCollection[Document] = prayugaDb.getCollection(PropertiesLoader.dbMasterCluster)

  def updateCentroidCluster(clusterSelected: String, newCentroid: Seq[String], to_ground: Double, angle_ground: Double, datetime: Long, newSize: Integer, newRadius: Double, link: String) = {
      masterClusterDb.updateOne(eq("cluster", clusterSelected), set("to_ground", to_ground))
  }
}
