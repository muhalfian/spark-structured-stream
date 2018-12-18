package com.muhalfian.spark

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.from_json
import org.bson._

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{explode, split}
// import org.apache.spark.sql.{functions => F}

import org.apache.lucene.analysis.id.IndonesianAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import scala.collection.mutable.ArrayBuffer


object MediaStream extends StreamUtils {

    val kafkaHost = "ubuntu"
    val kafkaPort = "9092"
    val topic = "online_media"
    val startingOffsets = "earliest"
    val kafkaBroker = kafkaHost+":"+kafkaPort

    val schema : StructType = StructType(Seq(
        StructField("link", StringType,true),
        StructField("source", StringType, true),
        StructField("authors", StringType, true),
        StructField("image", StringType, true),
        StructField("publish_date", StringType, true),
        StructField("title", StringType, true),
        StructField("text", StringType, true)
      )
    )

    val myschema = Seq(
        "link",
        "source",
        "authors",
        "image",
        "publish_date",
        "title",
        "text"
    )

    def main(args: Array[String]): Unit = {

        val spark = getSparkSession(args)
        import spark.implicits._

        spark.sparkContext.setLogLevel("ERROR")

        // read data stream from Kafka
        val kafka = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers",kafkaBroker)
            .option("subscribe", topic)
            .option("startingOffsets", startingOffsets)
            .load()

        // Transform data stream to Dataframe
        val kafkaDF = kafka.selectExpr("CAST(value AS STRING)").as[(String)]
            .select(from_json($"value", schema).as("data"))
            .select("data.*")

        // Preprocessing User Defined Function
        val preprocess = udf((content: String) => {
          val analyzer=new IndonesianAnalyzer()
          val tokenStream=analyzer.tokenStream("contents", content)
          //CharTermAttribute is what we're extracting
          val term=tokenStream.addAttribute(classOf[CharTermAttribute])

          tokenStream.reset() // must be called by the consumer before consumption to clean the stream

          var result = ArrayBuffer.empty[String]

          while(tokenStream.incrementToken()) {
              val termValue = term.toString
              if (!(termValue matches ".*[\\d\\.].*")) {
                result += term.toString
              }
          }
          tokenStream.end()
          tokenStream.close()
          result
        })

        // Preprocess Running in DF
        val preprocessDF = kafkaDF
            .withColumn("text_preprocess", preprocess(col("text").cast("string")))

        // Aggregate User Defined Function
        // val aggregate = udf((content: String) => {
        //     val splits = F.explode(F.split(content, " "))
        //     println(splits)
        // })

        // Aggregate Running in DF
        val aggregateDF = preprocessDF
            .withColumn("text_preprocess", aggregate(col("text_preprocess").cast("string")))


        // Show Data after processed
        // preprocessDF.writeStream
        //     .format("console")
        //     .option("truncate","false")
        //     .start()
        //     .awaitTermination()
    }



}
