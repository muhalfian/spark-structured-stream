package com.muhalfian.spark.models

import com.muhalfian.spark.jobs.OnlineStream
import com.muhalfian.spark.util._

import org.apache.spark.rdd.RDD
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD

import scala.collection.mutable.{ArrayBuffer, WrappedArray}
import org.apache.spark.sql._

object MasterClusterModel {

  // load spark
  val spark = OnlineStream.spark
  val sc = spark.sparkContext
  import spark.implicits._

  // master cluster
  val uri = PropertiesLoader.mongoUrl
  val db = PropertiesLoader.mongoDb
  val collection = "master_cluster_3"
  val masterCluster = MongoSpark.load(spark, ReadConfig(Map("uri" -> uri, "database" -> db, "collection" -> collection)))
  val masterClusterArr = getMasterClusterArr()
  var size = AggTools.masterWord.size
  var unknownCluster = getUnknownCluster()

  def getMasterClusterArr() = {
    val centroids = masterCluster.map(row => {
      (row.getAs[Seq[String]]("centroid"),row.getAs[Integer]("cluster"),row.getAs[Integer]("n"),row.getAs[Double]("radius"))
    }).collect

    ArrayBuffer(centroids: _*)
  }

  def getSize() = {
    masterClusterArr.size
  }

  def getUnknownCluster() = {
    masterClusterArr.map(data => {
      var centVec = ClusterTools.convertSeqToFeatures(data._1)
      var zeroVec = Array.fill(size)(0.01)
      var dist = ClusterTools.vlib.getDistance(centVec, zeroVec)
      (data._2, dist)
    }).minBy(_._2)._1
  }

  def getDmax() = {
    masterClusterArr.filter(x => x._2 != unknownCluster).maxBy(_._4)._4
  }

  def save(newDoc: RDD[org.bson.Document]) = {
    WriterUtil.saveBatchMongo(collection, newDoc)
  }
}
