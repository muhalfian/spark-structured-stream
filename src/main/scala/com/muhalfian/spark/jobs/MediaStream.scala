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
  var masterListAgg = ArrayBuffer[(String, Array[String])]()
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

    var currentPoint = 0

    preprocessDF.map(row => {
      val row1 = row.getAs[String]("text_preprocess")
      // val make = if (row1.toLowerCase == "125") "S" else "its 123"
      println("map text preprocess")
      (row(0),row1,row(0)) }).collect().foreach(println)


    val wordDict = udf((content: String, link: String) => {
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
          masterWords(point) += token
          masterWordsIndex += token
        }
        // println(link, currentPoint, count)
        // masterListAgg += ((link, splits.toArray))
      }
      println("Building Dictionary : " + masterWordsIndex.size)
      content
    })

    // Aggregate Running in DF
    val aggregateDF = preprocessDF
      .withColumn("text_preprocess", wordDict(col("text_preprocess").cast("string"), col("link").cast("string")))

    val aggregation = udf((content: String) => {

      val splits = content.split(" ").toSeq.map(_.trim).filter(_ != "")

      val intersectCounts: Map[String, Int] =
        masterWordsIndex.intersect(splits).map(s => s -> splits.count(_ == s)).toMap
      val wordCount: Array[Int] = masterWordsIndex.map(intersectCounts.getOrElse(_, 0)).toArray

      // println(wordCount.mkString(" "))
      println("Aggregate array : " + wordCount.size)
      masterAgg = masterAgg :+ wordCount

      content
    })

    val aggregateDF2 = aggregateDF
        .withColumn("text_preprocess", aggregation(col("text_preprocess").cast("string")))

    // masterWordsIndex.clear
    // for(row <- masterWords){
    //   masterWordsIndex = masterWordsIndex ++ row
    // }

    // // extract data from masterList
    // var groupMasterList = masterListAgg.groupBy(_._1)
    //
    // masterAgg = Vector[Array[Int]]()
    // for((group, splits) <- groupMasterList){
    //   val intersectCounts: Map[String, Int] =
    //     masterWordsIndex.intersect(splits).map(s => s -> splits.count(_ == s)).toMap
    //   val wordCount: Array[Int] = masterWordsIndex.map(intersectCounts.getOrElse(_, 0)).toArray
    //
    //   println(wordCount.mkString(" "))
    //   println("Size : " + wordCount.size)
    //   masterAgg = masterAgg :+ wordCount
    // }

    // =========================== SINK ====================================

    //Show Data after processed
    aggregateDF2.writeStream
      .format("console")
      // .option("truncate","false")
      .start()
      .awaitTermination()
  }



}
