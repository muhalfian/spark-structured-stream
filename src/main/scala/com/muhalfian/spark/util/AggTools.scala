package com.muhalfian.spark.util

import org.apache.spark.sql.functions.{split, col, udf}

import scala.collection.mutable.ArrayBuffer

// import org.apache.spark.ml.linalg._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object AggTools extends StreamUtils {

  // val indexWords = Map("a" -> 0, "b" -> 1, "c" -> 2,
  //                      "d" -> 3, "e" -> 4, "f" -> 5,
  //                      "g" -> 6, "h" -> 7, "i" -> 8,
  //                      "j" -> 9, "k" -> 10, "l" -> 11,
  //                      "m" -> 12, "n" -> 13, "o" -> 14,
  //                      "p" -> 15, "q" -> 16, "r" -> 17,
  //                      "s" -> 18, "t" -> 19, "u" -> 20,
  //                      "v" -> 21, "w" -> 22, "x" -> 23,
  //                      "y" -> 24, "z" -> 25)
  //
  // var countWords = 0
  // var masterLink = ArrayBuffer[String]()
  // val masterWords = ArrayBuffer.fill(26,1)(("",0))
  // var masterWordsIndex = ArrayBuffer[String]()
  // var masterWordsCount = ArrayBuffer[(String, Seq[(Int, Double)])]()
  // var temp : Seq[LabeledPoint] = Seq(LabeledPoint(0, Vectors.sparse(1, Seq((0, 0.0)))))
  // var masterAgg : Dataset[LabeledPoint] = temp.toDS
  // // var masterAgg = ArrayBuffer[Vector]()
  // // var masterListAgg = ArrayBuffer[(String, Int, Int)]()


}
