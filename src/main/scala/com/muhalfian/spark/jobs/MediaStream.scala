package com.muhalfian.spark.jobs

import com.muhalfian.spark.util._

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._

import scala.collection.mutable.{MutableList, ArrayBuffer, Set, HashSet}

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{explode, split, col, lit, concat, udf, from_json}

import org.apache.spark.ml.linalg._

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import org.apache.spark.sql.streaming.Trigger

import org.apache.spark.ml.clustering.BisectingKMeans


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

    val preprocessDF = filteredDF
                        .withColumn("text_preprocess", TextTools.stemming(col("text_preprocess")))

    val selectedDF = preprocessDF.select("link", "source", "authors", "image", "publish_date", "title", "text", "text_preprocess")
                        .withColumn("text_selected", TextTools.select(col("text_preprocess")))

    // ======================== AGGREGATION ================================

    val aggregateDF = selectedDF
      .withColumn("text_aggregate", AggTools.aggregate(col("text_selected"), col("link")))

    val customDF = aggregateDF
      .withColumn("text_aggregate", TextTools.stringify(col("text_aggregate").cast("string")))
      .withColumn("text_preprocess", TextTools.stringify(col("text_preprocess").cast("string")))
      .withColumn("text_selected", TextTools.stringify(col("text_selected").cast("string")))
      .withColumn("text", TextTools.stringify(col("text").cast("string")))

    // ============================ CLUSTERING =================================

    // val clustering = udf((content: String) => {
    //   // var masterAggUpdate = ArrayBuffer[Vector]()
    //   // val dimension = AggTools.masterWordsIndex.size
    //   //
    //   // for(row <- AggTools.masterAgg){
    //   //   var vecZeros = Vectors.zeros(dimension)
    //   //   var key = 0
    //   //   while(key < row.size){
    //   //     vecZeros.toArray(key) = row(key)
    //   //     key += 1
    //   //   }
    //   //   masterAggUpdate += vecZeros
    //   // }
    //
    //   // print("Dimension array - 0 : " + masterAggUpdate(0).size)
    //   content
    // })
    //
    // val clusterDF = aggregateDF
    //     .withColumn("text_aggregate", clustering(col("text_aggregate").cast("string")))

    // val kmeans = new BisectingKMeans().setK(3).setFeaturesCol("text_aggregate").setPredictionCol("prediction")
    // val model = kmeans.fit(aggregateDF)
    // val clusterDF = model.transform(aggregateDF)
    // // println(predicted.show)

    // =========================== SINK ====================================

    //Show Data after processed
    val printConsole = customDF.writeStream
      .format("console")
      // .option("truncate","false")
      .start()

    val saveMasterData = customDF
          .map(r => RowArtifact.rowMasterData(r))
          .writeStream
          .outputMode("append")
          .foreach(WriterUtil.masterData)
          .start()

    printConsole.awaitTermination()
    saveMasterData.awaitTermination()
  }

}
