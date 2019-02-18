package com.muhalfian.spark.jobs

import com.muhalfian.spark.util._

import org.bson.Document
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable.{MutableList, ArrayBuffer, Set, HashSet, WrappedArray}

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{explode, split, col, lit, concat, udf, from_json}

import org.apache.spark.ml.linalg._

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import org.apache.spark.sql.streaming.Trigger

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
// import org.apache.spark.ml.clustering.BisectingKMeans
// import com.muhalfian.spark.ml.BisectingKMeans


object GenerateModel extends StreamUtils {

  class AutomaticClustering extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function.
    override def inputSchema: StructType = ColsArtifact.preprocessSchema

    // This is the internal fields you keep for computing your aggregate.
    override def bufferSchema: StructType = StructType(Seq(
      StructField("matrix", ArrayType(DoubleType))
    ))

    // This is the output type of your aggregatation function.
    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    // This is the initial value for your buffer schema.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      println(s">>> initialize (buffer: $buffer)")
      buffer(0) = Array[Double]()
      // buffer(0) = 0L
      // buffer(1) = 1.0
    }

    // This is how to update your buffer schema given an input.
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      println(s">>> update (buffer: $buffer -> input: $input)")
      // buffer(0) = buffer.getAs[Long](0) + 1
      // buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](0)
      var content = input.getAs[WrappedArray[String]](0)

      var tempSeq = content.map(row => {
        var word = row.drop(1).dropRight(1).split("\\,")
        var index = AggTools.masterWordsIndex.indexWhere(_ == word(0))
        if(index == -1){
          AggTools.masterWordsIndex += word(0)
          index = AggTools.masterWordsIndex.size - 1
        }

        (index, word(1).toDouble)
      }).toSeq

      var vectorData = Vectors.sparse(AggTools.masterWordCount, tempSeq.sortWith(_._1 < _._1))
                      .toDense.toArray

      buffer(0) = vectorData
    }

    // This is how to merge two objects with the bufferSchema type.
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      println(s">>> merge (buffer1: $buffer1 -> buffer2: $buffer2)")
      // buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
      // buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
      buffer1(0) = buffer1.getAs[WrappedArray[Double]](0) :+ buffer2.getAs[WrappedArray[Double]](0)
    }

    // This is where you output the final value, given the final value of your bufferSchema.
    override def evaluate(buffer: Row): Any = {
      println(s">>> evaluate (buffer: $buffer)")
      var example = Array(Array(1.0, 2.0))
      example
      // math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))

    }
  }

  def main(args: Array[String]): Unit = {

    // ===================== LOAD SPARK SESSION ============================

    val spark = getSparkSession(args)
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    // ======================== READ STREAM ================================

    // read data stream from Kafka
    val kafka = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertiesLoader.kafkaBrokerUrl)
      .option("subscribePattern", "online_media.*")
      .option("startingOffsets", """{"online_media":{"0":0}}""")
      .option("endingOffsets", """{"online_media":{"0":10}}""")
      .load()

    // Transform data stream to Dataframe
    val kafkaDF = kafka.selectExpr("CAST(value AS STRING)").as[(String)]
      .select(from_json($"value", ColsArtifact.rawSchema).as("data"))
      .select("data.*")
      .withColumn("raw_text", concat(col("title"), lit(" "), col("text"))) // add column aggregate title and text

    // read master word
    val readConfig = ReadConfig(Map("uri" -> "mongodb://10.252.37.112/prayuga", "database" -> "prayuga", "collection" -> "master_word"))
    val masterWord = MongoSpark.load(spark, readConfig)
    AggTools.masterWordCount = masterWord.count.toInt

    // =================== PREPROCESS SASTRAWI =============================

    val regexDF = TextTools.regexTokenizer.transform(kafkaDF)

    val filteredDF = TextTools.remover.transform(regexDF)

    val preprocessDF = filteredDF
                        .withColumn("text_preprocess", TextTools.stemming(col("text_preprocess")))

    val selectedDF = preprocessDF.select("link", "source", "description", "image", "publish_date", "title", "text", "text_preprocess")
                        .withColumn("text_selected", TextTools.select(col("text_preprocess")))

    val df = selectedDF.withColumn("group", lit(0)).select("text_selected", "group")

    df.show()
    println(AggTools.masterWordCount)

    // ====================== UDAF =========================

    spark.udf.register("ac", new AutomaticClustering)

    // Create a DataFrame and Spark SQL table
    import org.apache.spark.sql.functions._

    // val ids = spark.range(1, 20)
    // ids.registerTempTable("ids")
    // val df = spark.sql("select id, id % 3 as group_id from ids")
    // df.registerTempTable("simple")

    df.show()

    // Or use Dataframe syntax to call the aggregate function.

    // Create an instance of UDAF GeometricMean.
    val ac = new AutomaticClustering

    // Show the geometric mean of values of column "id".
    df.groupBy("group").agg(ac(col("text_selected")).as("AutomaticClustering")).show()

    // // Invoke the UDAF by its assigned name.
    // df.groupBy("group_id").agg(expr("gm(id) as GeometricMean")).show()

  }
}
