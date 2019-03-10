package com.muhalfian.spark.jobs

import com.muhalfian.spark.util._

import org.bson.Document
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._

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

// import org.apache.spark.ml.clustering.BisectingKMeans
// import com.muhalfian.spark.ml.BisectingKMeans


object Dictionary extends StreamUtils {

  def main(args: Array[String]): Unit = {

    // ===================== LOAD SPARK SESSION ============================

    val spark = getSparkSession(args)
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    // ======================== READ STREAM ================================

    // read data stream from Kafka
    val kafka = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertiesLoader.kafkaBrokerUrl)
      .option("subscribePattern", "online_media.*")
      .option("startingOffsets", """{"online_media":{"0":-2}}""")
      .option("endingOffsets", """{"online_media":{"0":-1}}""")
      // .option("startingOffsets", """{"online_media":{"0":25500}}""")
      // .option("endingOffsets", """{"online_media":{"0":26000}}""")
      .load()

    // Transform data stream to Dataframe
    val kafkaDF = kafka.selectExpr("CAST(value AS STRING)").as[(String)]
      .select(from_json($"value", ColsArtifact.rawSchema).as("data"))
      .select("data.*")
      .withColumn("raw_text", concat(col("title"), lit(" "), col("text"))) // add column aggregate title and text

    // read master word
    val readConfig = ReadConfig(Map("uri" -> "mongodb://10.252.37.112/prayuga", "database" -> "prayuga", "collection" -> "master_word_2"))
    val masterWord = MongoSpark.load(spark, readConfig)
    var masterWordCount = masterWord.count.toInt
    masterWord.show()

    // =================== PREPROCESS SSparkSessionASTRAWI =============================

    val regexDF = TextTools.regexTokenizer.transform(kafkaDF)

    val filteredDF = TextTools.remover.transform(regexDF)

    val stemmedDF = filteredDF
                        .withColumn("text_stemmed", TextTools.stemming(col("text_filter")))

    val ngramDF = TextTools.ngram.transform(stemmedDF)

    val mergeDF = ngramDF.withColumn("text_preprocess", TextTools.merge(col("text_stemmed"), col("text_ngram_2")))

    val selectedDF = mergeDF.select("link", "source", "description", "image", "publish_date", "title", "text", "text_preprocess")
                        .withColumn("text_selected", TextTools.select(col("text_preprocess")))

    // ======================== SAVE DICTIONARY ================================

    val dictionary = spark.sparkContext.parallelize(selectedDF.rdd.flatMap(r => {
      var data = r.getAs[WrappedArray[String]](8).map( row => {
        masterWord.show()
        var word = row.drop(1).dropRight(1).split("\\,")
        var index = AggTools.masterWordsIndex.indexWhere(_ == word(0))
        if(index == -1){
          AggTools.masterWordsIndex += word(0)
          index = AggTools.masterWordsIndex.size - 1
        }
        val kata = word(0)
        println(s"doc save to mongodb : {index: $index, word: '$kata'}")
        Document.parse(s"{index: $index, word: '$kata'}")
      })
      data
    }).collect()).distinct

    val writeConfig = WriteConfig(Map("uri" -> "mongodb://10.252.37.112/prayuga", "database" -> "prayuga", "collection" -> "master_word_3"))
    MongoSpark.save(dictionary, writeConfig)

  }
}
