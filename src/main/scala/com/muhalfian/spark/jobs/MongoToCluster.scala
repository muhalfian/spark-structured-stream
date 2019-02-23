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
    val mongoDF = MongoSpark.load(sc, readConfig)



    // ======================== AGGREGATION ================================

    // val rows = selectedDF.count()
    // // val rddDF = selectedDF.flatMap(r => {
    // //   r.getAs[WrappedArray[String]](8).map( row => {
    // //     var word = row.drop(1).dropRight(1).split("\\,")
    // //     var index = AggTools.masterWordsIndex.indexWhere(_ == word(0))
    // //     if(index == -1){
    // //       AggTools.masterWordsIndex += word(0)
    // //       index = AggTools.masterWordsIndex.size - 1
    // //     }
    // //     word(0)
    // //   })
    // // })
    //
    // val rddDF = selectedDF.map(r => {
    //   r.getAs[WrappedArray[String]](8).map( row => {
    //     var word = row.drop(1).dropRight(1).split("\\,")
    //     var index = AggTools.masterWordsIndex.indexWhere(_ == word(0))
    //     if(index == -1){
    //       AggTools.masterWordsIndex += word(0)
    //       index = AggTools.masterWordsIndex.size - 1
    //     }
    //     AggTools.masterWordsIndex.size
    //   })
    // })
    // rddDF.collect()
    // rddDF.show()
    // println("counting " + AggTools.masterWordsIndex.size)
    //
    // val aggregateDF = selectedDF
    //   .withColumn("text_aggregate", AggTools.aggregate(col("text_selected"), col("link")))
    //
    // println("count row : " + selectedDF.count())
    //
    // val customDF = aggregateDF
    //   // .withColumn("text_aggregate", TextTools.stringify(col("text_aggregate").cast("string")))
    //   .withColumn("text_preprocess", TextTools.stringify(col("text_preprocess").cast("string")))
    //   // .withColumn("text_selected", TextTools.stringify(col("text_selected").cast("string")))
    //   .withColumn("text", TextTools.stringify(col("text").cast("string")))
    //
    //
    //
    // // customDF.select("link", "source", "description", "image", "publish_date", "title", "text", "text_preprocess", "text_aggregate").show()
    // val aggList = customDF.select("text_aggregate").rdd.map(r => {
    //   val row = r(0).asInstanceOf[DenseVector]
    //   // println("size : " + row.size)
    //   row
    // }).collect()
    // println(aggList)


  }

}
