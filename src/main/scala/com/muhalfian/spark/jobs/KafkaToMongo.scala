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

import com.mongodb.spark.config._
import com.mongodb.spark.MongoSpark

// import org.apache.spark.ml.clustering.BisectingKMeans
// import com.muhalfian.spark.ml.BisectingKMeans


object KafkaToMongo extends StreamUtils {

  def main(args: Array[String]): Unit = {

    // ===================== LOAD SPARK SESSION ============================

    val spark = getSparkSession(args)
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    // ======================== READ STREAM ================================

    // read data stream from Kafka
    val kafka = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertiesLoader.kafkaBrokerUrl)
      .option("subscribe", PropertiesLoader.kafkaTopic)
      .option("startingOffsets", """{"online_media":{"0":-2}}""")
      .option("endingOffsets", """{"online_media":{"0":10}}""")
      .load()

    // Transform data stream to Dataframe
    val kafkaDF = kafka.selectExpr("CAST(value AS STRING)").as[(String)]
      .select(from_json($"value", ColsArtifact.rawSchema).as("data"))
      .select("data.*")
      .withColumn("raw_text", concat(col("title"), lit(" "), col("text"))) // add column aggregate title and text

    kafkaDF.show()

    // =================== PREPROCESS SASTRAWI =============================

    val regexDF = TextTools.regexTokenizer.transform(kafkaDF)

    val filteredDF = TextTools.remover.transform(regexDF)

    val stemmedDF = filteredDF
                    .withColumn("text_stemmed", TextTools.stemming(col("text_filter")))

    val ngramDF = TextTools.ngram.transform(stemmedDF)

    val mergeDF = ngramDF.withColumn("text_preprocess", TextTools.merge(col("text_stemmed"), col("text_ngram_2")))

    val selectedDF = mergeDF
                    .select("link", "source", "description", "image", "publish_date", "title", "text", "text_preprocess", "text_html")
                    .withColumn("text_selected", TextTools.select(col("text_preprocess")))

    val aggregateDF = selectedDF
                    .withColumn("text_aggregate", AggTools.aggregateMongo(col("text_selected")))

    aggregateDF.show()

    // =========================== SINK ====================================

    MongoSpark.write(aggregateDF).mode("append").option("uri","mongodb://10.252.37.112/prayuga").option("collection","data_init_7_temp").save();

  }
}
