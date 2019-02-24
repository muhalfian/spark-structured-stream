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

    val readConfig = ReadConfig(Map("uri" -> "mongodb://10.252.37.112/prayuga", "database" -> "prayuga", "collection" -> "data_init"), Some(ReadConfig(sc)))
    val mongoRDD = MongoSpark.load(sc, readConfig)

    // ======================== AGGREGATION ================================

    val dict = 2500
    val aggregateArray = AggTools.aggregateBatch(mongoRDD, dict)

    // ======================== CLUSTERING ================================

    var method = "average"
    val n = 1000

    val clusterArray = ClusterTools.clib.AutomaticClustering(method, aggregateArray, n)
    val cluster = clusterArray.distinct

    ClusterTools.centroid = Array.ofDim[Double](clusterArray.size, dict)
    ClusterTools.distance = Array.ofDim[Double](clusterArray.size)
    ClusterTools.radius = Array.ofDim[Double](clusterArray.size)

    // get centroid each cluster
    ClusterTools.getCentroid(aggregateArray, clusterArray)

    // calculate distance
    ClusterTools.calculateDistance(aggregateArray, clusterArray)

    // calculate radius
    ClusterTools.calculateRadius(aggregateArray, clusterArray)

    // merge to masterData
    val masterData = ClusterTools.masterDataAgg(mongoRDD)

    // merge to masterCluster
    val masterCluster = sc.parallelize(ClusterTools.masterClusterAgg(mongoRDD, cluster))

    // ======================== WRITE MONGO ================================

    WriterUtil.saveBatchMongo("master_data",masterData)
    WriterUtil.saveBatchMongo("master_cluster",masterCluster)

  }

}
