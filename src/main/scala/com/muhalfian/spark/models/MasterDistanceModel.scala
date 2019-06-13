package com.muhalfian.spark.models

import com.muhalfian.spark.jobs.OnlineStream
import com.muhalfian.spark.util._

import org.apache.spark.rdd.RDD
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD

import scala.collection.mutable.{ArrayBuffer, WrappedArray}
import org.apache.spark.sql._

object MasterDistanceModel {

  // load spark
  val spark = OnlineStream.spark
  val sc = spark.sparkContext
  import spark.implicits._

  // master cluster
  val uri = PropertiesLoader.mongoUrl
  val db = PropertiesLoader.mongoDb
  val collectionRead = PropertiesLoader.dbMasterData
  val collectionWrite = PropertiesLoader.dbMasterDistance

  val masterData = MongoSpark.load(spark, ReadConfig(Map("uri" -> uri, "database" -> db, "collection" -> collectionRead)))
  val masterDistanceArr = getMasterDistanceArr()
  var size = MasterWordModel.masterWordArr.size
  var unknownCluster = getUnknownCluster()

  def getMasterDistanceArr() = {
    val distance = masterData
    .map(row => {
      (row.getAs[String]("link"),row.getAs[String]("text_aggregate"),row.getAs[String]("cluster"),getTimeStamp())
    }).collect

    // val groupedCentroids = centroids.groupBy(_._2).mapValues(_.maxBy(_._6)).map(_._2).toList

    ArrayBuffer(distance: _*)
  }

  def getTimeStamp() : Long = {
    val timestamp = java.lang.System.currentTimeMillis
    // val timestamp = java.lang.System.currentTimeMillis / 1000
    timestamp
  }

  def getSize() = {
    masterDistanceArr.size
  }

  def save(newDoc: RDD[org.bson.Document]) = {
    WriterUtil.saveBatchMongo(collectionWrite, newDoc)
  }
}
