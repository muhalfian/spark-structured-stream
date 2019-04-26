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
  val collectionRead = PropertiesLoader.dbMasterCluster
  val collectionWrite = PropertiesLoader.dbMasterClusterUpdate

  val masterCluster = MongoSpark.load(spark, ReadConfig(Map("uri" -> uri, "database" -> db, "collection" -> collectionRead)))
  val masterClusterArr = getMasterClusterArr()
  var size = MasterWordModel.masterWordArr.size
  var unknownCluster = getUnknownCluster()

  def getMasterClusterArr() = {
    val centroids = masterCluster.map(row => {
      (row.getAs[Seq[String]]("centroid"),row.getAs[String]("cluster"),row.getAs[Integer]("n"),row.getAs[Double]("radius"),row.getAs[String]("link_id"), row.getAs[String]("datetime"))
    }).collect

    // println(centroids)
    println(centroids.groupBy(_._2).mapValues(_.maxBy(_._6)).map(_._2).tolist)

    ArrayBuffer(centroids: _*)
  }

  def getSize() = {
    masterClusterArr.size
  }

  def getUnknownCluster() = {
    var unknownClusterId = "0"
    try {
      unknownClusterId = masterClusterArr.map(data => {
        var centVec = ClusterTools.convertSeqToFeatures(data._1)
        var zeroVec = Array.fill(size)(0.0)
        var dist = ClusterTools.vlib.getDistance(centVec, zeroVec)
        (data._2, dist)
      }).minBy(_._2)._1
    } catch {
      case _: Throwable =>  {
        unknownClusterId = "0"
      }
    }
    unknownClusterId
  }

  def getDmax() = {
    var dmax = 0.0
    try{
      dmax = masterClusterArr.filter(x => x._2 != unknownCluster).maxBy(_._4)._4
    } catch {
      case _: Throwable =>  {
        dmax = 0
      }
    }
    dmax
  }

  def save(newDoc: RDD[org.bson.Document]) = {
    WriterUtil.saveBatchMongo(collectionWrite, newDoc)
  }
}
