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


object BroStream extends StreamUtils {

    val kafkaHost = "ubuntu"
    val kafkaPort = "9092"
    val topic = "online_media"
    val startingOffsets = "earliest"
    val kafkaBroker = kafkaHost+":"+kafkaPort

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

        val kafkaData = kafka
            .withColumn("Key", $"key".cast(StringType))
            .withColumn("Topic", $"topic".cast(StringType))
            .withColumn("Offset", $"offset".cast(LongType))
            .withColumn("Partition", $"partition".cast(IntegerType))
            .withColumn("Timestamp", $"timestamp".cast(TimestampType))
            .withColumn("Value", $"value".cast(StringType))
            .select("Value")

        kafkaData.writeStream
            .outputMode("append")
            .format("console")
            // .option("truncate", false)
            .start()
            .awaitTermination()

    }
}
