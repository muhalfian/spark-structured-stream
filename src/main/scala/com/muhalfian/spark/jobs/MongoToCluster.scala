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

import com.mongodb.spark.config._
import com.mongodb.spark.MongoSpark

import scala.collection.JavaConversions._

// import org.apache.spark.ml.clustering.BisectingKMeans
// import com.muhalfian.spark.ml.BisectingKMeans


object MongoToCluster extends StreamUtils {

  def main(args: Array[String]): Unit = {

    // ===================== LOAD SPARK SESSION ============================

    val spark = getSparkSession(args)
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    // ======================== READ STREAM ================================

    val readConfig = ReadConfig(Map("uri" -> "mongodb://10.252.37.112/prayuga", "database" -> "prayuga", "collection" -> "data_init"), Some(ReadConfig(sc)))
    val mongoRDD = MongoSpark.load(sc, readConfig)

    // mongoRDD.collect().foreach(println)

    // ======================== AGGREGATION ================================

    val aggregateRDD = mongoRDD.map(r => {
      // var tempSeq = r.getAs[WrappedArray[String]](9).map( row => {
      var tempSeq = r.get("text_selected", new java.util.ArrayList[(String, Double)]())

      tempSeq = tempSeq.map( word => {
        // var word = row.drop(1).dropRight(1).split("\\,")
        var index = AggTools.masterWordsIndex.indexWhere(_ == word._1)
        if(index == -1){
          AggTools.masterWordsIndex += word._1
          index = AggTools.masterWordsIndex.size - 1
        }

        (index, word._2)
      }).asScala.toSeq

      val size = 2500
      val vectorData = Vectors.sparse(size, tempSeq.sortWith(_._1 < _._1)).toDense.toString
      tempSeq
    })

    aggregateRDD.collect().foreach(println)

  }

}
