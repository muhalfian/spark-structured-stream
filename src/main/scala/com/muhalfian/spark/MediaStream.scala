package com.muhalfian.spark

import java.io._
import java.net._
import java.util._

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

object MediaStream extends StreamUtils {

    val kafkaHost = "ubuntu"
    val kafkaPort = "9092"
    val topic = "online_media"
    val startingOffsets = "latest"
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

    def main(args: Array[String]): Unit = {

        val spark = getSparkSession(args)
        import spark.implicits._

        spark.sparkContext.setLogLevel("ERROR")

        val kafka = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers",kafkaBroker)
            .option("subscribe", topic)
            .option("startingOffsets", startingOffsets)
            .load()

        val kafkaDF = kafka.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
            .select(from_json($"value", schema).as("data"), $"timestamp")
            .select("data.*", "timestamp")

        val preprocessDF = kafkaDF
            .select("text")
            .foldLeft(kafkaDF){ (memoDF, colName) =>
                memoDF.withColumn(
                  "text_preprocess",
                  regexp_replace(col(colName), "\\s+", "")
                )
            }

        preprocessDF.writeStream
            .format("console")
            .option("truncate","false")
            .start()
            .awaitTermination()
    }
}
