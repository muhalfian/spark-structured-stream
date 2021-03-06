package com.muhalfian.spark.jobs

import com.muhalfian.spark.util._

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._

import scala.collection.mutable.{MutableList, ArrayBuffer, Set, HashSet, WrappedArray}

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{explode, split, col, lit, concat, udf, from_json}

import org.apache.spark.ml.linalg._

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import org.apache.spark.sql.streaming.Trigger

import scala.collection.JavaConversions._
import org.bson.Document

import com.muhalfian.spark.util._
import com.mongodb.spark.config._
import com.mongodb.spark.MongoSpark

// import org.apache.spark.ml.clustering.BisectingKMeans
// import com.muhalfian.spark.ml.BisectingKMeans


object MongoToCluster extends StreamUtils {

  def main(args: Array[String]): Unit = {

    // ===================== LOAD SPARK SESSION ============================

    val spark = getSparkSession(args)
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    // ======================== READ MONGO ================================

    val mongoUrl = PropertiesLoader.mongoUrl+PropertiesLoader.mongoDb

    val readWord = ReadConfig(Map("collection" -> PropertiesLoader.dbMasterWord), Some(ReadConfig(sc)))
    AggTools.masterWordCount = MongoSpark.load(sc, readWord).count.toInt

    val readConfig = ReadConfig(Map("collection" -> PropertiesLoader.dbDataInit), Some(ReadConfig(sc)))
    val mongoRDD = MongoSpark.load(sc, readConfig)

    // ======================== AGGREGATION ================================

    val dict = AggTools.masterWordCount
    // val dict = 5000
    println("dict : "+ dict)

    val aggregateArray = AggTools.mongoToArray(mongoRDD, dict)
    println("jumlah data aggregasi : " + aggregateArray.size )

    // ======================== CLUSTERING ================================

    var method = "average"
    val n = dict

    ClusterTools.clusterArray = ClusterTools.clib.AutomaticClustering(method, aggregateArray, n).map(_.toString)
    val cluster = ClusterTools.clusterArray.distinct
    println("jumlah data tercluster : " + ClusterTools.clusterArray.size )
    println("jumlah cluster : " + cluster.size )

    ClusterTools.centroid = Array.ofDim[Double](ClusterTools.clusterArray.size, dict)
    ClusterTools.distance = Array.ofDim[Double](ClusterTools.clusterArray.size)
    ClusterTools.radius = Array.ofDim[Double](ClusterTools.clusterArray.size)
    ClusterTools.n = Array.ofDim[Int](ClusterTools.clusterArray.size)

    // get centroid each cluster
    ClusterTools.getCentroid(aggregateArray, ClusterTools.clusterArray)

    // calculate distance
    ClusterTools.calculateDistance(aggregateArray, ClusterTools.clusterArray)

    // calculate radius
    ClusterTools.calculateRadius(aggregateArray, ClusterTools.clusterArray)

    // merge to masterData
    val masterData = ClusterTools.masterDataAgg(mongoRDD)

    val masterDistance = ClusterTools.masterDistanceAgg(mongoRDD)

    // merge to masterCluster
    val masterCluster = sc.parallelize(ClusterTools.masterClusterAgg(mongoRDD, cluster))

    val masterWord = sc.parallelize(AggTools.masterWordAgg())

    // ======================== WRITE MONGO ================================

    WriterUtils.saveBatchMongo(PropertiesLoader.dbMasterData,masterData)
    WriterUtils.saveBatchMongo(PropertiesLoader.dbMasterDistance,masterDistance)
    WriterUtils.saveBatchMongo(PropertiesLoader.dbMasterClusterUpdate,masterCluster)
    WriterUtils.saveBatchMongo(PropertiesLoader.dbMasterCluster,masterCluster)
    WriterUtils.saveBatchMongo(PropertiesLoader.dbMasterWord,masterWord)
  }
}
