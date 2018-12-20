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

import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}

import jsastrawi.morphology.{Lemmatizer, DefaultLemmatizer}
import scala.collection.mutable.{Set, HashSet}
import java.io.BufferedReader
import java.io.InputStreamReader


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

    var dictionary : Set[String] = HashSet.empty[String]

    // Memuat file kata dasar dari distribusi JSastrawi
    // Jika perlu, anda dapat mengganti file ini dengan kamus anda sendiri
    // InputStream in = Lemmatizer.class.getResourceAsStream("/root-words.txt");
    // BufferedReader br = new BufferedReader(new InputStreamReader(in));
    val filename = "/home/ubuntu/Documents/spark-structured-stream/src/main/scala/com/muhalfian/spark/data/kata-dasar.txt"
    var br = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(filename)))

    var line : String = ""
    while ((line = br.readLine()) != "") {
        dictionary.add(line)
    }

    var lemmatizer = new DefaultLemmatizer(dictionary);

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

        // // Preprocessing User Defined Function
        // val preprocess = udf((content: String) => {
        //     content
        // })
        //
        // // Preprocess Running in DF
        // val preprocessDF = kafkaDF
        //     .withColumn("text_preprocess", preprocess(col("text").cast("string")))

        // =================== PREPROCESS ===============================

        val tokenizer = new Tokenizer().setInputCol("text_preprocess").setOutputCol("text_preprocess")
        val regexTokenizer = new RegexTokenizer()
          .setInputCol("text")
          .setOutputCol("text_preprocess")
          .setPattern("\\w*[^\\W\\d]") // alternatively .setPattern("\\w+").setGaps(false)

        val regexTokenized = regexTokenizer.transform(kafkaDF)
        val tokenized = tokenizer.transform(regexTokenized)

        val stemming = udf((content: Seq[Seq[String]]) => {
            content.foreach{
              _.foreach{
                lemmatizer.lemmatize(_)
              }
            }
        })

        // Preprocess Running in DF
        val stemmed = tokenized
            .withColumn("text_preprocess", stemming(col("text_preprocess")))

        //
        // // Aggregate User Defined Function
        // val aggregate = udf((content: Column) => {
        //     val splits = explode(split(content, " "))
        //     println(splits)
        // })
        //
        // // Aggregate Running in DF
        // val aggregateDF = preprocessDF
        //     .withColumn("text_preprocess", aggregate(col("text_preprocess")))

        //Show Data after processed
        stemmed.writeStream
            .format("console")
            .option("truncate","false")
            .start()
            .awaitTermination()
    }



}
