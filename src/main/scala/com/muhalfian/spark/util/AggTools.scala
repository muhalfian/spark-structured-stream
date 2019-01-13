package com.muhalfian.spark.util

import org.apache.spark.sql.functions.{split, col, udf}

import scala.collection.mutable.ArrayBuffer

// import org.apache.spark.ml.linalg._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object AggTools extends StreamUtils {

  val indexWords = Map("a" -> 0, "b" -> 1, "c" -> 2,
                       "d" -> 3, "e" -> 4, "f" -> 5,
                       "g" -> 6, "h" -> 7, "i" -> 8,
                       "j" -> 9, "k" -> 10, "l" -> 11,
                       "m" -> 12, "n" -> 13, "o" -> 14,
                       "p" -> 15, "q" -> 16, "r" -> 17,
                       "s" -> 18, "t" -> 19, "u" -> 20,
                       "v" -> 21, "w" -> 22, "x" -> 23,
                       "y" -> 24, "z" -> 25)


  var masterWordsIndex = ArrayBuffer[String]()

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

    println(tempSeq)
    println(masterWordsIndex.size)
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


}
