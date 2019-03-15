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

  val spark = getSparkSessionPlain()
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  // val masterWord = MongoSpark.load(OnlineStream.spark)
  // masterWord.show()
  // var masterWordCount = masterWord.count.toInt

  def main(args: Array[String]): Unit = {

    // ===================== LOAD SPARK SESSION ============================

    // val spark = getSparkSession(args)
    // import spark.implicits._
    // spark.sparkContext.setLogLevel("ERROR")

    // ======================== READ STREAM ================================

    // read data stream from Kafka
    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertiesLoader.kafkaBrokerUrl)
      .option("subscribe", PropertiesLoader.kafkaTopic)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "100")
      .load()

    // Transform data stream to Dataframe
    val kafkaDF = kafka.selectExpr("CAST(value AS STRING)").as[(String)]
      .select(from_json($"value", ColsArtifact.rawSchema).as("data"))
      .select("data.*")
      .withColumn("raw_text", concat(col("title"), lit(" "), col("text"))) // add column aggregate title and text

    // =================== PREPROCESS SSparkSessionASTRAWI =============================

    val regexDF = TextTools.regexTokenizer.transform(kafkaDF)

    val filteredDF = TextTools.remover.transform(regexDF)

    val stemmedDF = filteredDF
                        .withColumn("text_stemmed", TextTools.stemming(col("text_filter")))

    val ngramDF = TextTools.ngram.transform(stemmedDF)

    val mergeDF = ngramDF.withColumn("text_preprocess", TextTools.merge(col("text_stemmed"), col("text_ngram_2")))

    val selectedDF = mergeDF.select("link", "source", "description", "image", "publish_date", "title", "text", "text_preprocess")
                        .withColumn("text_selected", TextTools.select(col("text_preprocess")))

    // // ======================== AGGREGATION ================================
    //
    val aggregateDF = selectedDF
      .withColumn("text_aggregate", AggTools.aggregateMongo(col("text_selected")))

    val clusterDF = aggregateDF
      .withColumn("new_cluster", ClusterTools.onlineClustering(col("text_aggregate")))
      .withColumn("to_centroid", ClusterTools.updateRadius(col("text_aggregate"),col("new_cluster")))

    //
    // val aggregateDF = selectedDF.map( d => {
    //   d.getAs[WrappedArray[String]](8).map( row => {
    //     masterWord.show()
    //     var word = row.drop(1).dropRight(1).split("\\,")
    //     var index = masterWord.filter($"word" === word(0)).rdd.map(r => r.getInt(1)).collect.toList(0)
    //
    //     // var index = Try(
    //     //
    //     //         ).getOrElse(
    //     //           masterWordCount
    //     //         )
    //
    //     if(index == masterWordCount){
    //       masterWordCount += 1
    //     } else {
    //       index = 0
    //       word = null
    //     }
    //
    //     val kata = word(0)
    //     var query = s"{index: $index, word: '$kata'}"
    //     println(s"doc save to mongodb : {index: $index, word: '$kata'}")
    //     // Document.parse(query)
    //     row
    //   })
    // })

    //
    // val customDF = aggregateDF
    //   .withColumn("text_aggregate", TextTools.stringify(col("text_aggregate").cast("string")))
    //   .withColumn("text_preprocess", TextTools.stringify(col("text_preprocess").cast("string")))
    //   .withColumn("text_selected", TextTools.stringify(col("text_selected").cast("string")))
    //   .withColumn("text", TextTools.stringify(col("text").cast("string")))

    // ====================== ONLINE CLUSTERING ================================



    // =========================== SINK ====================================


    //Show Data after processed
    val printConsole = clusterDF.writeStream
      .format("console")
      // .option("truncate","false")
      .start()


    val saveMasterData = clusterDF
          .map(r => RowArtifact.rowMasterDataUpdate(r))
          .writeStream
          .outputMode("append")
          .foreach(WriterUtil.masterDataUpdate)
          .start()

    printConsole.awaitTermination()
    saveMasterData.awaitTermination()
  }

}
