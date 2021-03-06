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

import org.bson.Document
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._

// import org.apache.spark.ml.clustering.BisectingKMeans
// import com.muhalfian.spark.ml.BisectingKMeans


object OnlineStream extends StreamUtils {

  // ===================== LOAD SPARK SESSION ============================

  val spark = getSparkSessionPlain()
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {

    // ======================== READ STREAM ================================

    // read data stream from Kafka
    val kafka = spark
      // .read
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertiesLoader.kafkaBrokerUrl)
      .option("subscribe", PropertiesLoader.kafkaTopic)
      .option("startingOffsets", "latest")
      .option("maxOffsetsPerTrigger", "1")
      // .option("startingOffsets", """{"online_media":{"0":4000}}""")
      // .option("endingOffsets", """{"online_media":{"0":6000}}""")
      .load()

    // Transform data stream to Dataframe
    val kafkaDF = kafka.selectExpr("CAST(value AS STRING)").as[(String)]
      .select(from_json($"value", ColsArtifact.rawSchema).as("data"))
      .select("data.*")
      .withColumn("raw_text", concat(col("title"), lit(" "), col("text"))) // add column aggregate title and text

    // =================== PREPROCESS SASTRAWI =============================

    val regexDF = TextTools.regexTokenizer.transform(kafkaDF)

    val filteredDF = TextTools.remover.transform(regexDF)

    val stemmedDF = filteredDF
                    .withColumn("text_stemmed", TextTools.stemming(col("text_filter")))

    val ngramDF = TextTools.ngram.transform(stemmedDF)

    val selectedDF = ngramDF
      .withColumn("text_preprocess", TextTools.merge(col("text_stemmed"), col("text_ngram_2")))
      .select("link", "source", "description", "image", "publish_date", "title", "text", "text_html", "text_preprocess")
      .withColumn("text_selected", TextTools.select(col("text_preprocess")))

    // ======================== AGGREGATION ================================

    val aggregateDF = selectedDF
      .withColumn("text_aggregate", AggTools.aggregateMongo(col("text_selected")))

    val clusterDF = aggregateDF
      .withColumn("cluster", ClusterTools.onlineClustering(col("text_aggregate"),col("link")))
      .withColumn("to_centroid", ClusterTools.updateRadius(col("text_aggregate"),col("cluster")))

    val customDF = clusterDF
      .withColumn("text_aggregate", TextTools.stringify(col("text_aggregate").cast("string")))
      .withColumn("text_preprocess", TextTools.stringify(col("text_preprocess").cast("string")))
      .withColumn("text_selected", TextTools.stringify(col("text_selected").cast("string")))
      .withColumn("text", TextTools.stringify(col("text").cast("string")))
      .map(r => RowArtifact.rowMasterDataUpdate(r))

    // =========================== SINK ====================================



    val saveMasterData = customDF
          .writeStream
          .outputMode("append")
          .foreach(WriterUtils.masterDataUpdate)
          .start()

   //Show Data after processed
   val printConsole = customDF.writeStream
          .format("console")
          // .option("truncate","false")
          .start()

    saveMasterData.awaitTermination()
    printConsole.awaitTermination()

  }

}
