package com.muhalfian.spark.util

import org.apache.spark.sql.functions.{split, col, udf}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.linalg._

object AggTools {
  val indexWords = Map("a" -> 0, "b" -> 1, "c" -> 2,
                       "d" -> 3, "e" -> 4, "f" -> 5,
                       "g" -> 6, "h" -> 7, "i" -> 8,
                       "j" -> 9, "k" -> 10, "l" -> 11,
                       "m" -> 12, "n" -> 13, "o" -> 14,
                       "p" -> 15, "q" -> 16, "r" -> 17,
                       "s" -> 18, "t" -> 19, "u" -> 20,
                       "v" -> 21, "w" -> 22, "x" -> 23,
                       "y" -> 24, "z" -> 25)

  val masterWords = ArrayBuffer.fill(26,1)("")
  var masterWordsIndex = ArrayBuffer[String]()
  var masterAgg = ArrayBuffer[Vector]()
  var masterListAgg = ArrayBuffer[ArrayBuffer[String]]()

  val aggregate = udf((content: String) => {
    val splits = content.split(" ").toSeq.map(_.trim).filter(_ != "")

    val grouped = splits.groupBy(identity)

    for ((token,tokenArr) <- grouped) {
      var point = indexWords(token.take(1))

      var currentPoint = masterWords(point).indexWhere(_ == token)
      if(currentPoint == -1){
        masterWords(point) += token
        masterWordsIndex += token
      }
      // println(link, currentPoint, count)

    }

    masterListAgg += (splits.to[ArrayBuffer])

    masterAgg.clear
    for(row <- masterListAgg){
      val intersectCounts: Map[String, Int] =
        masterWordsIndex.intersect(row).map(s => s -> row.count(_ == s)).toMap
      val wordCount = Vectors.dense(masterWordsIndex.map(intersectCounts.getOrElse(_, 0)).map(_.toDouble).toArray)

      // println(wordCount.mkString(" "))
      println("Aggregate array : " + wordCount.size)
      masterAgg = masterAgg :+ wordCount
    }
    content
  })
}
