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



// import org.apache.lucene.analysis.id.IndonesianAnalyzer
// import org.apache.lucene.analysis.tokenattributes.CharTermAttribute


object MediaStream extends StreamUtils {

  // aggregation
  // var masterWords = new Array[String](78000)
  val masterWords = ArrayBuffer.fill(26,1)("")
  var masterWordsIndex = ArrayBuffer[String]()
  var masterAgg = Vector[Array[Int]]()

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


    val indexWords = Map("a" -> 0, "b" -> 1, "c" -> 2,
                         "d" -> 3, "e" -> 4, "f" -> 5,
                         "g" -> 6, "h" -> 7, "i" -> 8,
                         "j" -> 9, "k" -> 10, "l" -> 11,
                         "m" -> 12, "n" -> 13, "o" -> 14,
                         "p" -> 15, "q" -> 16, "r" -> 17,
                         "s" -> 18, "t" -> 19, "u" -> 20,
                         "v" -> 21, "w" -> 22, "x" -> 23,
                         "y" -> 24, "z" -> 25)

    var masterListAgg = MutableList[(String, Int, Int)]()

    var currentPoint = 0

    val wordDict = udf((content: String, link: String) => {
      var edited = false
      val splits = content.split(" ")
        .toSeq
        .map(_.trim)
        .filter(_ != "")

      val counted = splits.groupBy(identity).mapValues(_.size)

      for ((token,count) <- counted) {
        var char = token.take(1)
        // println(token + " -> " + char)

        var point = indexWords(char)

        var currentPoint = masterWords(point).indexWhere(_ == token)
        if(currentPoint == -1){
          edited = true
          masterWords(point) += token
          currentPoint = masterWords(point).indexWhere(_ == token)
        }
        // println(link, currentPoint, count)
        // masterListAgg += ((link, currentPoint, count))
      }

      if(edited){
        masterWordsIndex.clear
        for(row <- masterWords){
          masterWordsIndex = masterWordsIndex ++ row
        }
        edited = false
        println(masterWordsIndex.mkString(" "))
      }

      // extract data from List
      // var groupMasterList = masterListAgg.groupBy(_._1)
      // // print(groupMasterList)
      //
      // for((group, content) <- groupMasterList){
      //     var temp = Array.fill[Int](78000)(0)
      //
      //     for(row <- content){
      //         temp(row._2) = row._3
      //     }
      //     masterAgg += temp
      //     // println(temp)
      // }

      content
    })

    // Aggregate Running in DF
    val aggregateDF = preprocessDF
      .withColumn("text_preprocess", wordDict(col("text_preprocess").cast("string"), col("link").cast("string")))

    val aggregate = udf((content: String, link: String) => {
      val splits = content.split(" ").toSeq.map(_.trim).filter(_ != "")

      // val counted = splits.groupBy(identity).mapValues(_.size)

      // val temp = Array.empty[Type]()
      val intersectCounts: Map[String, Int] =
        masterWordsIndex.intersect(splits).map(s => s -> splits.count(_ == s)).toMap

      val wordCount: Array[String] = masterWordsIndex.map(intersectCounts.getOrElse(_, 0))

      masterAgg = masterAgg :+ wordCount

    })

    val aggregateDF2 = aggregateDF
      .withColumn("text_preprocess", aggregate(col("text_preprocess").cast("string"), col("link").cast("string")))
      // .withColumn("text_aggregate", aggregate(col("text_preprocess").cast("string")))

    // var wordRDD =  preprocessDF.select("text_preprocess").
    //                             flatMap( row => {
    //                                 row.split(" ")
    //                             } ).
    //                             map( word => word ).
    //                             reduceByKey( _ )
    //                             // sortBy( z => (z._2, z._1), ascending = false )
    //
    // println(wordRDD)

    // =========================== SINK ====================================

    //Show Data after processed
    aggregateDF2.writeStream
      .format("console")
      // .option("truncate","false")
      .start()
      .awaitTermination()
  }



}
