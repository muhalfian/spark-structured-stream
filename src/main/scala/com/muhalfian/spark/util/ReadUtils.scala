package com.muhalfian.spark.util

import com.muhalfian.spark.jobs.OnlineStream

import org.bson.Document
import org.apache.spark.rdd.RDD
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._

import org.apache.spark.sql._

object ReadUtils {
  val uri = PropertiesLoader.mongoUrl
  val db = PropertiesLoader.mongoDb

  val spark = OnlineStream.spark
  val sc = spark.sparkContext
  import spark.implicits._

  def readMongo(collection: String) : RDD[Row] = {
    val readConfig = ReadConfig(Map("uri" -> uri, "database" -> db, "collection" -> collection))
    MongoSpark.load(OnlineStream.spark, readConfig)
  }
}
