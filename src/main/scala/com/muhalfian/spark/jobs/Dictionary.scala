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
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertiesLoader.kafkaBrokerUrl)
      .option("subscribePattern", "online_media.*")
      .option("startingOffsets", """{"online_media":{"0":-2}}""")
      .option("endingOffsets", """{"online_media":{"0":25500}}""")
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

    val selectedDF = preprocessDF.select("link", "source", "description", "image", "publish_date", "title", "text", "text_preprocess")
                        .withColumn("text_selected", TextTools.select(col("text_preprocess")))

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
    // val masterWordList = new ArrayBuffer[Document]

    // val writeConfig = WriteConfig(Map("collection" -> "master_word", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))

    val rddDF = spark.parallelize(selectedDF.map(r => {
      var data = r.getAs[WrappedArray[String]](8).map( row => {
        var word = row.drop(1).dropRight(1).split("\\,")
        var index = AggTools.masterWordsIndex.indexWhere(_ == word(0))
        if(index == -1){
          AggTools.masterWordsIndex += word(0)
          index = AggTools.masterWordsIndex.size - 1
        }
        AggTools.masterWordsIndex.size
        (index, word)
      })

      Document.parse(s"{index: $data(0), word: $data(1)}")
    })
  )

    rddDF.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://10.252.37.112/master_word")))

    // MongoSpark.save(masterWordList)
    // rddDF.collect()
    // rddDF.show()
    // println("counting " + AggTools.masterWordsIndex.size)

    // //Show Data after processed
    // val printConsole = customDF.write
    //   .format("console")
    //   // .option("truncate","false")
    //   .start()
    //
    // println(customDF)
    //
    // val saveMasterWord = selectedDF
    //       .select("text_selected")
    //       .map(r => r.getAs[WrappedArray[String]](0))
    //       .writeStream
    //       .outputMode("append")
    //       .foreach(WriterUtil.masterWord)
    //       .start()
    //
    // // printConsole.awaitTermination()
    // saveMasterWord.awaitTermination()
  }

}
