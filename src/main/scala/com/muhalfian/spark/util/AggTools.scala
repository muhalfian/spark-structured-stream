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

  var countWords = 0

  // var masterLink = ArrayBuffer[String]()
  val masterWords = ArrayBuffer.fill(26,1)(("",0))
  var masterWordsIndex = ArrayBuffer[String]()
  // var masterWordsCount = ArrayBuffer[(String, Seq[(Int, Double)])]()
  // var temp : Seq[LabeledPoint] = Seq(LabeledPoint(0, Vectors.sparse(1, Seq((0, 0.0)))))
  // var masterAgg : Dataset[LabeledPoint] = temp.toDS
  // var seqLabel = Seq[LabeledPoint]()

  val aggregate = udf((content: Seq[String], link: String) => {
    // val splits = content.split(" ").toSeq.map(_.trim).filter(_ != "")

    val grouped = content.groupBy(identity).mapValues(_.size)
    var tempSeq = Seq[(Int, Double)]()

    for ((token,count) <- grouped) {
      var point = indexWords(token.take(1))

      var index = masterWords(point).indexWhere(_._1 == token)
      var currentPoint = 0
      if(index == -1){
        masterWordsIndex += token
        currentPoint = masterWordsIndex.size - 1
        masterWords(point) += ((token, currentPoint))

      } else {
        currentPoint = masterWords(point)(index)._2
      }

      tempSeq = tempSeq :+ (currentPoint, count.toDouble)
    }

    // masterLink += link

    countWords = masterWordsIndex.size

    val vectorData : Vector = Vectors.sparse(countWords, tempSeq.sortWith(_._1 < _._1))

    // seqLabel = seqLabel :+ LabeledPoint(masterLink.size-1, Vectors.sparse(countWords, tempSeq))
    // var dataset: Dataset[LabeledPoint] = temp.toDS
    //
    // println(dataset.select("*").show(false))
    // masterAgg = masterAgg.union(dataset)
    println("aggregate " + countWords)
    // content
    vectorData
  })


}
