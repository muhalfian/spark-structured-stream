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

import org.apache.lucene.analysis.id.IndonesianAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import scala.collection.mutable.ArrayBuffer

import jsastrawi.morphology.{Lemmatizer, DefaultLemmatizer}
import scala.collection.mutable.{Set, HashSet}
// import java.io.BufferedReader
// import java.io.InputStreamReader
import scala.io.Source
import collection.JavaConverters._
// import java.util.{Set, HashSet}


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

    val dictionary : Set[String] = HashSet[String]()
    //
    // // Memuat file kata dasar dari distribusi JSastrawi
    // // Jika perlu, anda dapat mengganti file ini dengan kamus anda sendiri
    // // InputStream in = Lemmatizer.class.getResourceAsStream("/root-words.txt");
    // // BufferedReader br = new BufferedReader(new InputStreamReader(in));
    val filename = "/home/blade1/Documents/spark-structured-stream/src/main/scala/com/muhalfian/spark/data/kata-dasar.txt"
    // var br = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(filename)))

    // var line : String = ""
    // while ((line = br.readLine()) != "") {
    //     dictionary.add(line)
    // }
    for (line <- Source.fromFile(filename).getLines) {
        dictionary.add(line)
    }
    val dict : java.util.Set[String] = dictionary.asJava

    var lemmatizer = new DefaultLemmatizer(dict);

    def main(args: Array[String]): Unit = {

        val spark = getSparkSession(args)
        import spark.implicits._

        spark.sparkContext.setLogLevel("ERROR")

        // ======================== READ STREAM ================================

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

        // ==================== PREPROCESS APACHE LUCENE =======================

        // val preprocess = udf((content: String) => {
        //     val analyzer=new IndonesianAnalyzer()
        //     val tokenStream=analyzer.tokenStream("contents", content)
        //     val term=tokenStream.addAttribute(classOf[CharTermAttribute]) //CharTermAttribute is what we're extracting
        //
        //     tokenStream.reset() // must be called by the consumer before consumption to clean the stream
        //
        //     // var result = ArrayBuffer.empty[String]
        //     var result = ""
        //
        //     while(tokenStream.incrementToken()) {
        //         val termValue = term.toString
        //         if (!(termValue matches ".*[\\d\\.].*")) {
        //             result += term.toString + " "
        //         }
        //     }
        //     tokenStream.end()
        //     tokenStream.close()
        //     result
        // })

        // val preprocessDF = kafkaDF
        //     .withColumn("text_preprocess", preprocess(col("text").cast("string")))

        // ===================== PREPROCESS SASTRAWI ===========================


        val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("text_preprocess")
        val regexTokenizer = new RegexTokenizer()
          .setInputCol("text")
          .setOutputCol("text_preprocess")
          .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

        val countTokens = udf { (words: Seq[String]) => {
            words.foreach{
              lemmatizer.lemmatize(_)
            }
        } }

        val preprocessDF = tokenizer.transform(sentenceDataFrame)
        preprocessDF.select("text", "text_preprocess")
            .withColumn("tokens", countTokens(col("text_preprocess"))).show(false)

        // val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
        // regexTokenized.select("sentence", "words")
        //     .withColumn("tokens", countTokens(col("words"))).show(false)

        // val regexTokenizer = new RegexTokenizer()
        //   .setInputCol("text_preprocess")
        //   .setOutputCol("text_preprocess")
        //   .setPattern("\\w*[^\\W\\d]") // alternatively .setPattern("\\w+").setGaps(false)
        //
        // val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("text_preprocess")
        //
        // val tokenized = tokenizer.transform(kafkaDF)
        //
        // // val regexTokenized = regexTokenizer.transform(tokenized)
        //
        // val stemming = udf((content: Seq[Seq[String]]) => {
        //     content.foreach{
        //       _.foreach{
        //         lemmatizer.lemmatize(_)
        //       }
        //     }
        // })
        //
        // // Preprocess Running in DF
        // val preprocessDF = tokenized
        //     .withColumn("text_preprocess", stemming(col("text_preprocess")))

        // ======================== AGGREGATION ================================

        // // Aggregate User Defined Function
        // val aggregate = udf((content: Column) => {
        //     val splits = explode(split(content, " "))
        //     println(splits)
        // })
        //
        // // Aggregate Running in DF
        // val aggregateDF = preprocessDF
        //     .withColumn("text_preprocess", aggregate(col("text_preprocess")))

        // =========================== SINK ====================================

        //Show Data after processed
        preprocessDF.writeStream
            .format("console")
            // .option("truncate","false")
            .start()
            .awaitTermination()
    }



}
