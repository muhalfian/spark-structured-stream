package com.muhalfian.spark.util

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

// private[spark] trait StreamUtils {
class StreamUtils extends Serializable {

  def getSparkContext(args: Array[String]): SparkContext = {
    getSparkSession(args).sparkContext
  }

  def getSparkSession(args: Array[String]): SparkSession = {
    val mongoUrl = PropertiesLoader.mongoUrl + PropertiesLoader.mongoDb + "." + PropertiesLoader.dbDataInit
    val uri: String = args.headOption.getOrElse(mongoUrl)
    // val master: String = "spark://10.252.37.109:7077"
    val master: String = "local[2]"

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("PrayugaStream")
      .set("spark.app.id", "StreamProtocolCountToMongo")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)
      // // 1 workers
      // .set("spark.executor.instances", "1")
      // // 1 cores on each workers
      // .set("spark.executor.cores", "1");

    val session = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // MongoConnector(session.sparkContext).withDatabaseDo(WriteConfig(session), {db => db.drop()})
    session
  }

  def getSparkSessionPlain(): SparkSession = {
    val uri: String = PropertiesLoader.mongoUrl + PropertiesLoader.mongoDb + "." + PropertiesLoader.dbDataInit
    // val master: String = "spark://10.252.37.109:7077"
    val master: String = "local[3]"

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("PrayugaStream")
      .set("spark.app.id", "StreamProtocolCountToMongo")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)
      // 1 workers
      // .set("spark.executor.instances", "1")

    val session = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // MongoConnector(session.sparkContext).withDatabaseDo(WriteConfig(session), {db => db.drop()})
    session
  }
}
