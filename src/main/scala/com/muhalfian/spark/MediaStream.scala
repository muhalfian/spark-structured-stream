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

class Document(
    var link : String,
    var source : String,
    var authors : String,
    var image : String,
    var publish_date : String,
    var title : String,
    var text : String,
    var text_preprocess : String
) extends Serializable


class Documents(var documents: List[Document] = new ArrayList[Document]()) extends Serializable{
    def add(link: String, source: String, authors: String, image: String, publish_date: String, title: String, text: String, text_preprocess: String = ""){
        documents.add(new Document(link, source, authors, image, publish_date, title, text, text_preprocess))
    }

    def add(doc : Document){
        documents.add(doc)
    }
}

object MediaStream extends StreamUtils {

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

        // val kafkaData = kafka
        //     .withColumn("Key", $"key".cast(StringType))
        //     .withColumn("Topic", $"topic".cast(StringType))
        //     .withColumn("Offset", $"offset".cast(LongType))
        //     .withColumn("Partition", $"partition".cast(IntegerType))
        //     .withColumn("Timestamp", $"timestamp".cast(TimestampType))
        //     .withColumn("Value", $"value".cast(StringType))
        //     .select("Value")
        //
        // kafkaData.writeStream
        //     .outputMode("append")
        //     .format("console")
        //     // .option("truncate", false)
        //     .start()
        //     .awaitTermination()

        // Preparing a dataframe with Content and Sentiment columns
        val streamingDataFrame = kafka.selectExpr("cast (value as string) AS Content").withColumn("text", preprocess($"Content"))

        // Displaying the streaming data
        streamingDataFrame.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

    }

    val preprocess = udf((textContent: String)=>{
        var inputDocs = new Documents()
        inputDocs.add(textContent,textContent,textContent,textContent,textContent,textContent,textContent)
        inputDocs.text_preprocess = "preprocess" + inputDocs.text_preprocess
    })
}
