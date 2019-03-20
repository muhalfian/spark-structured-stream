package com.muhalfian.spark.util

import org.apache.spark.sql.functions.{split, col, udf}

import scala.collection.mutable.{ArrayBuffer, WrappedArray}

import com.muhalfian.spark.jobs.OnlineStream

import org.bson.Document
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._

// import org.apache.spark.ml.linalg._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

import org.apache.spark.rdd.RDD
import org.bson.Document
import scala.collection.JavaConversions._
import scala.util.Try

object AggTools {

  val spark = OnlineStream.spark
  val sc = spark.sparkContext
  import spark.implicits._

  var masterWordsIndex = ArrayBuffer[String]()
  var masterWordCount = 0

  val aggregateMongo = udf((content: Seq[String]) => {
    var tempSeq = content.map(row => {
      var word = row.drop(1).dropRight(1).split("\\,")

      var index = 0
      var indexStat = MasterWordModel.getIndex(word(0))
      if(indexStat == -1){
        index = MasterWordModel.addMasterWord(word(0))
      } else {
        index = masterWord(indexStat)._2
      }

      println("word : " + word(0) + "(" + index + ") - " + word(1).toDouble)
      (index, word(1).toDouble)
    }).toSeq

    println(tempSeq)

    val vectorData = tempSeq.sortWith(_._1 < _._1)
    vectorData.map(_.toString)
  })

  def mongoToArray(mongoRDD:RDD[org.bson.Document], size:Int): Array[Array[Double]] = {
    val aggregateArray = mongoRDD.map(r => {
      var tempJava = r.get("text_aggregate", new java.util.ArrayList[String]())

      var tempSeq = tempJava.map( row => {
        var word = row.drop(1).dropRight(1).split("\\,")
        (word(0).toInt, word(1).toDouble)
      }).toSeq

      val vectorData = Vectors.sparse(size, tempSeq.sortWith(_._1 < _._1)).toDense.toArray

      vectorData
    }).collect()

    println(aggregateArray)
    aggregateArray
  }


  def masterWordAgg(): ArrayBuffer[org.bson.Document] = {

    val masterWord = masterWordsIndex.zipWithIndex.map( row => {
      val word = row._1
      val index = row._2
      Document.parse(s"{word : '$word', index: $index}")
    })

    masterWord
  }
}
