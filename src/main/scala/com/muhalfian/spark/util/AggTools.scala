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

  // read master word
  val readConfig = ReadConfig(Map("uri" -> "mongodb://10.252.37.112/prayuga", "database" -> "prayuga", "collection" -> "master_word_2"))
  var masterWord : Array[(String, Integer)] = MongoSpark.load(spark, readConfig).select("word", "index").map(row => {
    (row.getAs[String](0),row.getAs[Integer](1))
  }).collect
  var masterWordIndex = ArrayBuffer(masterWord: _*)


  val aggregate = udf((content: Seq[String], link: String) => {

    var tempSeq = content.map(row => {
      var word = row.drop(1).dropRight(1).split("\\,")
      var index = masterWordsIndex.indexWhere(_ == word(0))
      if(index == -1){
        masterWordsIndex += word(0)
        index = masterWordsIndex.size - 1
      }

      (index, word(1).toDouble)
    }).toSeq

    println(masterWordsIndex.size)

    // println(tempSeq)
    // println(masterWordsIndex.size)
    // val vectorData = Vectors.sparse(masterWordsIndex.size, tempSeq.sortWith(_._1 < _._1)).toDense
    val vectorData = Vectors.sparse(masterWordsIndex.size, tempSeq.sortWith(_._1 < _._1)).toDense.toString

    // val vectorData = ""
    // seqLabel = seqLabel :+ LabeledPoint(masterLink.size-1, Vectors.sparse(countWords, tempSeq))
    // var dataset: Dataset[LabeledPoint] = temp.toDS
    //
    // println(dataset.select("*").show(false))
    // masterAgg = masterAgg.union(dataset)
    println("aggregate " + masterWordsIndex.size)
    // content
    vectorData
  })

  val aggregateMongo = udf((content: Seq[String]) => {

    println("=========================== UDF ===============================")
    // masterWord.foreach(println)

    var tempSeq = content.map(row => {
      var word = row.drop(1).dropRight(1).split("\\,")

      var index = 0
      var indexStat = masterWordIndex.indexWhere(_._1 == word(0))
      if(indexStat == -1){
        println("add to database : " + word(0))
        index = masterWordIndex.size - 1
        // masterWord = masterWord ++ new Array[(String, Integer)]((word(0), index))
        // masterWord = masterWord ++ Array((((word(0), index))))
        // println(masterWord ++ Array((word(0), index)))
        // masterWord ++ Array((word(0), index))
        masterWordIndex += ((word(0), index))

      } else {
        index = masterWordIndex(indexStat)._2
      }

      // if(index == masterWordCount){
      //   masterWordCount += 1
      //
      //   // save new word to mongodb
      //   val writeConfig = WriteConfig(Map("uri" -> "mongodb://10.252.37.112/prayuga", "database" -> "prayuga", "collection" -> "master_word_2"))
      //   println(s"doc save to mongodb : {index: $index, word: '$word'}")
      //   val newWord = sc.parallelize(
      //     Seq(Document.parse(s"{index: $index, word: '$word'}"))
      //   )
      //   MongoSpark.save(newWord, writeConfig)
      // }

      println("word : " + word(0) + "(" + index + ") - " + word(1).toDouble)
      (index, word(1).toDouble)
    }).toSeq

    println(tempSeq)
    // println(masterWordsIndex.size)

    // val vectorData = Vectors.sparse(masterWordCount, tempSeq.sortWith(_._1 < _._1)).toDense.toString

    // println("aggregate " + masterWordsIndex.size)
    // vectorData
    content
  })

  def aggregateBatch(mongoRDD:RDD[org.bson.Document], size:Int): Array[Array[Double]] = {
    val aggregateArray = mongoRDD.map(r => {
      var tempJava = r.get("text_selected", new java.util.ArrayList[String]())

      var tempSeq = tempJava.map( row => {
        var word = row.drop(1).dropRight(1).split("\\,")
        var index = masterWordsIndex.indexWhere(_ == word(0))
        if(index == -1){
          masterWordsIndex += word(0)
          index = masterWordsIndex.size - 1
        }

        (index, word(1).toDouble)
      }).toSeq

      val vectorData = Vectors.sparse(size, tempSeq.sortWith(_._1 < _._1)).toDense.toArray
      vectorData
    }).collect()

    aggregateArray
  }

  def initDictionary(mongoRDD:RDD[org.bson.Document]): Int = {
    var temp = mongoRDD.map(r => {
      r.get("text_selected", new java.util.ArrayList[String]())
        .map( row => {
          var word = row.drop(1).dropRight(1).split("\\,")
          var index = masterWordsIndex.indexWhere(_ == word(0))
          if(index == -1){
            masterWordsIndex += word(0)
            index = masterWordsIndex.size - 1
          }
        })
    }).collect()

    println(temp)

    masterWordsIndex.size
  }

  def masterWordAgg(): ArrayBuffer[org.bson.Document] = {
    // val masterWord = masterWordsIndex.map{ word =>
    //   Document.parse(s"{word : '$word'}")
    // }
    val masterWord = masterWordsIndex.zipWithIndex.map( row => {
      val word = row._1
      val index = row._2
      Document.parse(s"{word : '$word', index: $index}")
    })

    masterWord
  }
}
