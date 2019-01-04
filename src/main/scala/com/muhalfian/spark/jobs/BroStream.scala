package com.muhalfian.spark.jobs

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.from_json
import org.bson._
import com.muhalfian.spark.util.StreamUtils

import scala.collection.JavaConverters._
import scala.collection.mutable



object BroStream extends StreamUtils {
    case class ConnCountObj(
      link: String,
      source: String,
      authors: String,
      image: String,
      publish_date: String,
      title: String,
      text: String
    )

    def main(args: Array[String]): Unit = {
      val kafkaUrl = "ubuntu:9092"
      val topic = "online_media"

      val spark = getSparkSession(args)
      import spark.implicits._

      spark.sparkContext.setLogLevel("ERROR")
      val kafkaStreamDF = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",kafkaUrl)
        .option("subscribe", topic)
        .option("startingOffsets","earliest")
        .load()

      val kafkaStream = kafkaStreamDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)]

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

      val parsedLogData = kafkaStreamDF
         .select(col("value")
         .cast(StringType)
         .as("col")
      )
      .select(from_json(col("col"), schema)
        .alias("conn")
      )

      val parsedRawDf = parsedLogData.select("conn.*")
      val textDf = parsedLogData.select("conn.text")

      val connDf = parsedRawDf
        .map((r:Row) => ConnCountObj(
          r.getAs[String](0),
          r.getAs[String](1),
          r.getAs[String](2),
          r.getAs[String](3),
          r.getAs[String](4),
          r.getAs[String](5),
          r.getAs[String](6)
        ))

      // println(kafkaStreamDF)
      // println(kafkaStream)
      // println(parsedLogData)
      // print(parsedRawDf)
      // println(connDf)
      // println(textDf)
      // println(connDf.show())

      var printConsole = connDf.writeStream
        .format("console")
        // .option("truncate","false")
        .start()


      //Sink to Mongodb
      val ConnCountQuery = connDf
          .writeStream
          .outputMode("append")
          .foreach(new ForeachWriter[ConnCountObj] {

              val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://10.252.37.112/spark.broisot"))
              var mongoConnector: MongoConnector = _
              var ConnCounts: mutable.ArrayBuffer[ConnCountObj] = _

              override def process(value: ConnCountObj): Unit = {
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
                      doc
                    }).asJava)
                  })
                }
              }

              override def open(partitionId: Long, version: Long): Boolean = {
                mongoConnector = MongoConnector(writeConfig.asOptions)
                ConnCounts = new mutable.ArrayBuffer[ConnCountObj]()
                true
              }

            }).start()
      //
      //   // Sink to HDFS
      //   val parsedRawToHDFSQuery = parsedLogData
      //        .writeStream
      //        .option("checkpointLocation", "hdfs://blade1-node:9000/checkpoint/stream/bro")
      //        .option("path","hdfs://blade1-node:9000/input/spark/stream/bro")
      //        .outputMode("append")
      //        .format("json")
      //        .start()

        ConnCountQuery.awaitTermination()
        printConsole.awaitTermination()
        // parsedRawToHDFSQuery.awaitTermination()
    }
}
