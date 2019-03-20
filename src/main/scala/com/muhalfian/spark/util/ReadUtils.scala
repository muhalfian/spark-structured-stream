package com.muhalfian.spark.util

import com.muhalfian.spark.jobs.OnlineStream

// import org.bson.Document
import org.apache.spark.rdd.RDD
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD

// import org.mongodb.scala.Document

import org.apache.spark.sql._

object ReadUtils {
  val uri = PropertiesLoader.mongoUrl
  val db = PropertiesLoader.mongoDb

  val spark = OnlineStream.spark
  val sc = spark.sparkContext
  import spark.implicits._

  val collection = "master_cluster_3"
  val readConfig = ReadConfig(Map("uri" -> uri, "database" -> db, "collection" -> collection))
  val readCluster = MongoSpark.load(spark, readConfig)

  def readMongo(collection: String) = {
    val readConfig = ReadConfig(Map("uri" -> uri, "database" -> db, "collection" -> collection))
    val load = MongoSpark.load(sc, readConfig)
    // load.show()
    load
  }
}
