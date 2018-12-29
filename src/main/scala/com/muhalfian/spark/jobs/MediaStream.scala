package com.muhalfian.spark.jobs

import com.muhalfian.spark.util._

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._

import scala.collection.mutable.{MutableList, ArrayBuffer, Set, HashSet}

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{explode, split, col, lit, concat, udf, from_json}

import org.apache.spark.ml.linalg._

// import org.apache.lucene.analysis.id.IndonesianAnalyzer
// import org.apache.lucene.analysis.tokenattributes.CharTermAttribute


object MediaStream extends StreamUtils {

  def main(args: Array[String]): Unit = {

    // ===================== LOAD SPARK SESSION ============================

    val spark = getSparkSession(args)
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    // ======================== READ STREAM ================================

    // read data stream from Kafka
    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertiesLoader.kafkaBrokerUrl)
      .option("subscribe", PropertiesLoader.kafkaTopic)
      .option("startingOffsets", PropertiesLoader.kafkaStartingOffset)
      .load()

    // Transform data stream to Dataframe
    val kafkaDF = kafka.selectExpr("CAST(value AS STRING)").as[(String)]
      .select(from_json($"value", ColsArtifact.rawSchema).as("data"))
      .select("data.*")
      .withColumn("raw_text", concat(col("title"), lit(" "), col("text"))) // add column aggregate title and text


    // =================== PREPROCESS SASTRAWI =============================

    val regexDF = TextTools.regexTokenizer.transform(kafkaDF)

    val filteredDF = TextTools.remover.transform(regexDF)

    val preprocessDF = filteredDF.select("link", "source", "authors", "image", "publish_date", "title", "text", "text_preprocess")
                              .withColumn("text_preprocess", TextTools.stemming(col("text_preprocess").cast("string")))

    // ======================== AGGREGATION ================================

    val aggregateDF = preprocessDF
      .withColumn("text_preprocess", AggTools.aggregate(col("text_preprocess").cast("string")))

    // ============================ CLUSTERING =================================

    val clustering = udf((content: String) => {
      // var masterAggUpdate = ArrayBuffer[Vector]()
      // val dimension = AggTools.masterWordsIndex.size
      //
      // for(row <- AggTools.masterAgg){
      //   var vecZeros = Vectors.zeros(dimension)
      //   var key = 0
      //   while(key < row.size){
      //     vecZeros.toArray(key) = row(key)
      //     key += 1
      //   }
      //   masterAggUpdate += vecZeros
      // }

      // print("Dimension array - 0 : " + masterAggUpdate(0).size)
      content
    })

    val clusterDF = aggregateDF
        .withColumn("text_preprocess", clustering(col("text_preprocess").cast("string")))

    // =========================== SINK ====================================

    //Show Data after processed
    clusterDF.writeStream
      .format("console")
      // .option("truncate","false")
      .start()
      .awaitTermination()
  }



}
