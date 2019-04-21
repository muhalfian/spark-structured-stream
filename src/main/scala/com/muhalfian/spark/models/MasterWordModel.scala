package com.muhalfian.spark.models

import com.muhalfian.spark.jobs.OnlineStream
import com.muhalfian.spark.util._

import org.apache.spark.rdd.RDD
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD
import org.bson.Document

import scala.collection.mutable.{ArrayBuffer, WrappedArray}
import org.apache.spark.sql._

object MasterWordModel {

  // load spark
  val spark = OnlineStream.spark
  val sc = spark.sparkContext
  import spark.implicits._

  // master cluster
  val uri = PropertiesLoader.mongoUrl
  val db = PropertiesLoader.mongoDb
  val collectionRead = PropertiesLoader.dbMasterWord
  val collectionWrite = PropertiesLoader.dbMasterWord

  val masterWord = MongoSpark.load(spark, ReadConfig(Map("uri" -> uri, "database" -> db, "collection" -> collectionRead)))
  val masterWordArr = initMasterWord()

  def initMasterWord() = {
    var masterWordData = ArrayBuffer[(String, Integer)]()
    try {
      masterWordData = getMasterWordArr()
    } catch {
      case _: Throwable =>  {
        masterWordData += (("null",0))
        val newWord = sc.parallelize(Seq(Document.parse(s"{index: 0, word: 'null'}")))
        save(newWord)
      }
    }
    masterWordData
  }

  def getMasterWordArr() = {
    val words = masterWord.select("word", "index").map(row => {
      (row.getAs[String](0),row.getAs[Integer](1))
    }).collect
    ArrayBuffer(words: _*)
  }

  def save(newWord: RDD[org.bson.Document]) = {
    WriterUtil.saveBatchMongo(collectionWrite, newWord)
  }

  def getIndex(word: String) = {
    masterWordArr.indexWhere(_._1 == word)
  }

  def addMasterWord(word: String) = {
    println("add to database : " + word)

    var index = masterWordArr.size
    masterWordArr += ((word, index))

    // Mongo save
    val newWord = sc.parallelize(Seq(Document.parse(s"{index: $index, word: '$word'}")))
    save(newWord)

    index
  }
}
