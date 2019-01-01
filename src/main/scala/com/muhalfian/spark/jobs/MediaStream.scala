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

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

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


    // =================== PREPROCESS SSparkSessionASTRAWI =============================

    val regexDF = TextTools.regexTokenizer.transform(kafkaDF)

    val filteredDF = TextTools.remover.transform(regexDF)

    // val preprocessDF = filteredDF.select("link", "source", "authors", "image", "publish_date", "title", "text", "text_preprocess")
    //                           .withColumn("text_preprocess", TextTools.stemming(col("text_preprocess").cast("string")))
    val preprocessDF = filteredDF.select("link", "source", "authors", "image", "publish_date", "title", "text", "text_preprocess")
                              .withColumn("text_preprocess", TextTools.stemming(col("text_preprocess")))

    // ======================== AGGREGATION ================================

    // val aggregateDF = preprocessDF
    //   .withColumn("text_preprocess", AggTools.aggregate(col("text_preprocess").cast("string")))

    val indexWords = Map("a" -> 0, "b" -> 1, "c" -> 2,
                         "d" -> 3, "e" -> 4, "f" -> 5,
                         "g" -> 6, "h" -> 7, "i" -> 8,
                         "j" -> 9, "k" -> 10, "l" -> 11,
                         "m" -> 12, "n" -> 13, "o" -> 14,
                         "p" -> 15, "q" -> 16, "r" -> 17,
                         "s" -> 18, "t" -> 19, "u" -> 20,
                         "v" -> 21, "w" -> 22, "x" -> 23,
                         "y" -> 24, "z" -> 25)

    var countWords = 0

    var masterLink = ArrayBuffer[String]()
    val masterWords = ArrayBuffer.fill(26,1)(("",0))
    var masterWordsIndex = ArrayBuffer[String]()
    var masterWordsCount = ArrayBuffer[(String, Seq[(Int, Double)])]()
    // var temp : Seq[LabeledPoint] = Seq(LabeledPoint(0, Vectors.sparse(1, Seq((0, 0.0)))))
    // var masterAgg : Dataset[LabeledPoint] = temp.toDS
    // var seqLabel = Seq[LabeledPoint]()

    val aggregate = udf((content: Seq[String], link: String) => {
      // val splits = content.split(" ").toSeq.map(_.trim).filter(_ != "")

      val grouped = content.groupBy(identity).mapValues(_.size)
      var tempSeq = Seq[(Int, Double)]()

      for ((token,count) <- grouped) {
        var point = indexWords(token.take(1))

        var index = masterWords(point).indexWhere(_._1 == token)
        var currentPoint = 0
        if(index == -1){
          masterWordsIndex += token
          currentPoint = masterWordsIndex.size - 1
          masterWords(point) += ((token, currentPoint))

        } else {
          currentPoint = masterWords(point)(index)._2
        }

        tempSeq = tempSeq :+ (currentPoint, count.toDouble)
      }

      // masterLink += link

      countWords = masterWordsIndex.size

      val vectorData = Vectors.sparse(countWords, tempSeq)

      // seqLabel = seqLabel :+ LabeledPoint(masterLink.size-1, Vectors.sparse(countWords, tempSeq))
      // var dataset: Dataset[LabeledPoint] = temp.toDS
      //
      // println(dataset.select("*").show(false))
      // masterAgg = masterAgg.union(dataset)
      println("aggregate " + countWords)
      // content
      vectorData
    })

    val aggregateDF = preprocessDF
      .withColumn("text_aggregate", aggregate(col("text_preprocess"), col("link").cast("string")))

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

    val kmeans = new KMeans().setK(3).setFeaturesCol("text_aggregate").setPredictionCol("prediction")
    val model = kmeans.fit(aggregateDF)
    val predicted = model.transform(aggregateDF)
    println(predicted.show)

    // =========================== SINK ====================================

    //Show Data after processed
    clusterDF.writeStream
      .format("console")
      // .option("truncate","false")
      .start()
      .awaitTermination()
  }



}
