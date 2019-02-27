package com.muhalfian.spark.util

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

private[spark] trait StreamUtils {
  def getSparkContext(args: Array[String]): SparkContext = {
    getSparkSession(args).sparkContext
  }

  def getSparkSession(args: Array[String]): SparkSession = {
    val uri: String = args.headOption.getOrElse("mongodb://10.252.37.112/spark.bro15jun")
    // val master: String = "spark://10.252.37.109:7077"
    val master: String = "local[*]"

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("PrayugaStream")
      .set("spark.app.id", "StreamProtocolCountToMongo")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)

    val session = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    MongoConnector(session.sparkContext).withDatabaseDo(WriteConfig(session), {db => db.drop()})
    session
  }

  def getSparkSessionPlain(): SparkSession = {
    val uri: String = args.headOption.getOrElse("mongodb://10.252.37.112/spark.bro15jun")
    // val master: String = "spark://10.252.37.109:7077"
    val master: String = "local[*]"

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("PrayugaStream")
      .set("spark.app.id", "StreamProtocolCountToMongo")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)

    val session = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // MongoConnector(session.sparkContext).withDatabaseDo(WriteConfig(session), {db => db.drop()})
    session
  }
}
