package com.muhalfian.spark.jobs

import com.muhalfian.spark.util._

// import com.mongodb.client.MongoCollection
// import com.mongodb.spark.MongoConnector
// import com.mongodb.spark.config.WriteConfig
//
// import org.apache.spark.{SparkContext, SparkConf}
// import org.apache.spark.sql._
//
// import scala.collection.mutable.{MutableList, ArrayBuffer, Set, HashSet}
//
// import org.apache.spark.sql.types._
// import org.apache.spark.sql.functions.{explode, split, col, lit, concat, udf, from_json}
//
// import org.apache.spark.ml.linalg._
//
// import org.apache.spark.ml.linalg.Vectors
// import org.apache.spark.ml.feature.LabeledPoint
// import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
//
// import org.apache.spark.sql.streaming.Trigger
//
// // import org.apache.spark.ml.clustering.{BisectingKMeans, KMeans
// import org.apache.spark.ml.clustering.BisectingKMeans
//
// import org.bson._
// import scala.collection.JavaConverters._

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.from_json
import org.bson._

import scala.collection.JavaConverters._
import scala.collection.mutable._


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
      .option("maxOffsetsPerTrigger", "500")
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

    val aggregateDF = preprocessDF
      .withColumn("text_aggregate", AggTools.aggregate(col("text_preprocess"), col("link").cast("string")))

    val customDF = aggregateDF
      .withColumn("text_aggregate", TextTools.stringify(col("text_aggregate").cast("string")))
      .withColumn("text_preprocess", TextTools.stringify(col("text_preprocess").cast("string")))
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

    // val aggregateSave = customDF
    //   .select("link", "text_aggregate")
    //   .writeStream
    //   .option("checkpointLocation", "hdfs://blade1-node:9000/checkpoint/online_media/aggregation")
    //   .option("path","hdfs://blade1-node:9000/online_media/aggregation")
    //   .outputMode("append")
    //   .format("json")
    //   // .option("data", "/home/blade1/Documents/spark-structured-stream/data/")
    //   // .option("truncate","false")
    //   .start()
    //
    // val masterSave = customDF
    //   .select("link", "source", "authors", "image", "publish_date", "title", "text", "text_preprocess")
    //   .writeStream
    //   .option("checkpointLocation", "hdfs://blade1-node:9000/checkpoint/online_media/master")
    //   .option("path","hdfs://blade1-node:9000/online_media/master")
    //   .outputMode("append")
    //   .format("json")
    //   // .option("data", "/home/blade1/Documents/spark-structured-stream/data/")
    //   // .option("truncate","false")
    //   .start()

    //Sink to Mongodb
    val ConnCountQuery = customDF
          .writeStream
          .outputMode("append")
          .foreach(new ForeachWriter[ColsArtifact.ConnCountObj] {

              val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://10.252.37.112/spark.broisot"))
              var mongoConnector: MongoConnector = _
              var ConnCounts: ArrayBuffer[ColsArtifact.ConnCountObj] = _

              override def process(value: ColsArtifact.ConnCountObj): Unit = {
                ConnCounts.append(value)
              }

              override def close(errorOrNull: Throwable): Unit = {
                if (ConnCounts.nonEmpty) {
                  mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
                    collection.insertMany(ConnCounts.map(sc => {
                      var doc = new Document()
                      doc.put("link", sc.link)
                      doc.put("source", sc.source)
                      doc.put("authors", sc.authors)
                      doc.put("image", sc.authors)
                      doc.put("publish_date", sc.publish_date)
                      doc.put("title", sc.title)
                      doc.put("text", sc.text)
                      doc.put("text_preprocess", sc.text_preprocess)
                      doc.put("text_aggregate", sc.text_aggregate)
                      doc
                    }).asJava)
                  })
                }
              }

              override def open(partitionId: Long, version: Long): Boolean = {
                mongoConnector = MongoConnector(writeConfig.asOptions)
                ConnCounts = new ArrayBuffer[ColsArtifact.ConnCountObj]()
                true
              }
            }).start()

    printConsole.awaitTermination()
    // masterSave.awaitTermination()
    // aggregateSave.awaitTermination()
  }



}
