package com.muhalfian.spark.util

import com.muhalfian.spark.jobs.OnlineStream

import org.bson.Document
import org.apache.spark.rdd.RDD
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._

object ReadUtils {
  val uri = PropertiesLoader.mongoUri
  val db = PropertiesLoader.mongoDb

  def readMongo(collection: String) : RDD[Document] = {
    val readConfig = ReadConfig(Map("uri" -> uri, "database" -> db, "collection" -> collection))
    MongoSpark.load(OnlineStream.spark, readConfig)
  }
}
