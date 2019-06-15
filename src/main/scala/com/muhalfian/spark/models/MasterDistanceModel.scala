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
  println(masterData)
  val masterDistanceArr = getMasterDistanceArr()
  var size = MasterWordModel.masterWordArr.size

  def getMasterDistanceArr() = {
    val distance = masterData
    .map(row => {
      println((row.getAs[String]("link"),row.getAs[String]("text_aggregate").split("\\,").toSeq,row.getAs[String]("cluster"),row.getAs[Double]("to_centroid"),getTimeStamp()))
      (row.getAs[String]("link"),row.getAs[String]("text_aggregate").split("\\,").toSeq,row.getAs[String]("cluster"),row.getAs[Double]("to_centroid"),getTimeStamp())
    }).rdd
    println(distance)
    println(distance.size)
    val distanceCol = distance.collect
    println(distanceCol)

    // val groupedCentroids = centroids.groupBy(_._2).mapValues(_.maxBy(_._6)).map(_._2).toList
    ArrayBuffer(distanceCol: _*)
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
    WriterUtils.saveBatchMongo(collectionWrite, newDoc)
  }
}
