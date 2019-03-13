package com.muhalfian.spark.util

import com.muhalfian.spark.jobs.OnlineStream

import ALI._
import org.bson.Document
import org.apache.spark.rdd.RDD

import org.bson.Document
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._

import org.apache.spark.sql.functions.{split, col, udf}

import scala.collection.mutable.{ArrayBuffer, WrappedArray}

import org.apache.spark.ml.linalg.{Vector, Vectors}


object ClusterTools {
  var clusterArray = Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 4, 5, 0, 0, 1, 0, 0, 0, 0, 0, 6, 0, 7, 8, 0, 0, 6, 9, 0, 0, 0, 0, 9,
0, 0, 10, 0, 11, 0, 0, 7, 0, 0, 0, 10, 0, 0, 0, 12, 0, 13, 0, 0, 0, 0, 0, 0, 0, 7, 0, 14, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 7, 7, 0, 0, 7, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0, 17, 0, 18, 0, 0, 0, 19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 22, 0, 0, 0, 0, 0, 0, 23, 0, 0, 24, 0, 0, 0, 0, 0, 0, 23, 0, 25, 0, 0, 0, 1, 0, 0, 0, 0, 0, 26, 0, 0, 0, 0, 0, 0, 0, 27, 0, 0, 0, 28, 0, 0, 0, 0, 0, 0, 0, 29, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 30, 0, 0, 31, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 33, 0, 0, 0, 0, 0, 34, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 35, 0, 1, 0, 36, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 37, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 38, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 39, 0, 0, 39, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 40, 0, 0, 41, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 43, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 44, 0, 0, 0, 0, 45, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 46, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 47, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0, 49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 51, 0, 0, 52, 0, 53, 7, 0, 0, 0, 0, 23, 0, 0, 0, 0, 0, 54, 55, 0, 56, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 52, 0, 57, 0, 0, 0, 0, 7, 0, 0, 58, 59, 0, 0, 60, 0, 0, 0, 0, 0, 0, 0, 0, 61, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 62, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 65, 0, 0, 0, 0, 0, 0, 0, 0, 0, 66, 0, 7, 0, 0, 0, 0, 67, 0, 68, 0, 69, 0, 0, 0, 70, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 71, 0, 0, 0, 0, 0, 0, 72, 0, 0, 72, 0, 5, 0, 0, 0, 0, 0, 73, 0, 5, 0, 0, 74, 7, 0, 0, 0, 0, 0, 0, 0, 0, 75, 0, 0, 0, 7, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 76, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 77, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 78, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 7, 0, 0, 0, 0, 53, 0, 0, 0, 0, 0, 0, 0)

  val clib : ClusteringLib = new ClusteringLib();
  val vlib: VectorLib = new VectorLib()

  var centroid = Array[Array[Double]](Array(1.0))
  var distance = Array.ofDim[Double](1)
  var radius = Array.ofDim[Double](1)
  var n = Array.ofDim[Int](1)

  // MongoConfig
  val writeConfig = WriteConfig(Map("uri" -> "mongodb://10.252.37.112/prayuga", "database" -> "prayuga", "collection" -> "master_cluster_4"))
  val readConfig = ReadConfig(Map("uri" -> "mongodb://10.252.37.112/prayuga", "database" -> "prayuga", "collection" -> "master_cluster_4"))

  val spark = OnlineStream.spark
  val sc = spark.sparkContext
  import spark.implicits._

  // read master cluster
  // var centroidArr = MongoSpark.load(spark, readConfig).collect
  // centroidArr.foreach(println)
  var centroidArr = MongoSpark.load(spark, readConfig)
  .map(row => {
    (row.getAs[Seq[String]]("centroid"),row.getAs[Integer]("cluster"),row.getAs[Integer]("n"),row.getAs[Double]("radius"))
  }).collect
  centroidArr.foreach(println)

  // calculate outlier
  var size = AggTools.masterWord.size
  var outlier = centroidArr.map(data => {
    var cent = data._1.map( row => {
      var word = row.drop(1).dropRight(1).split("\\,")
      (word(0).toInt, word(1).toDouble)
    }).toSeq
    // var cent = centTupple.drop(1).dropRight(1).split("\\,")
    var centVec = Vectors.sparse(size, cent.sortWith(_._1 < _._1)).toDense.toArray
    var zeroVec = Vectors.sparse(size, Seq((0,0.0))).toDense.toArray
    var dist = 1 - CosineSimilarity.cosineSimilarity(centVec, zeroVec)
    (data._2, dist)
  })
  outlier.foreach(println)
  // .maxBy(_._2)._2
  // println("cluster outlier : " + outlier)

  // calculate rmax
  var rmax = centroidArr.filter(x => x._2 != 0).maxBy(_._4)._4
  println(rmax)

  // masterWord = ArrayBuffer(words: _*)

  def getCentroid(aggregateArray: Array[Array[Double]] , clusterArray: Array[Int] ) = {
    // merge cluster, array
    var dataArray = aggregateArray.zipWithIndex.map(data => {
      (clusterArray(data._2), data._1)
    }).groupBy(_._1)

    // find centroid
    for ((key, value) <- dataArray) {
      val data = value.map(arr => arr._2)
      centroid(key) = clib.getCentroid(data)
    }
  }

  def calculateDistance(aggregateArray: Array[Array[Double]] , clusterArray: Array[Int]) = {
    for ( i <- 1 to (aggregateArray.length - 1) ) {
      val cent = centroid(clusterArray(i))
      val data = aggregateArray(i)
      // distance(i) =  vlib.getDistance(cent, data)
      var dist = 1 - CosineSimilarity.cosineSimilarity(cent, data)
      if(dist < 0) {
        distance(i) = 0
      } else {
        distance(i) = dist
      }
    }
  }

  def calculateRadius(aggregateArray: Array[Array[Double]] , clusterArray: Array[Int] ) = {
    // merge cluster, array
    var dataArray = aggregateArray.zipWithIndex.map(data => {
      (clusterArray(data._2), distance(data._2))
    }).groupBy(_._1)

    // find radius
    for ((key, value) <- dataArray) {
      val dist = value.map(arr => arr._2)
      radius(key) = dist.max
      n(key) = dist.size
    }
  }

  def masterDataAgg(mongoRDD: RDD[org.bson.Document]) : RDD[org.bson.Document] = {
    val masterData = mongoRDD.zipWithIndex.map( row => {
      row._1.put("cluster", clusterArray(row._2.toInt))
      row._1.put("to_centroid", distance(row._2.toInt))
      row._1
    })
    masterData
  }

  def masterClusterAgg(mongoRDD : RDD[org.bson.Document], cluster: Array[Int]): Array[org.bson.Document] = {
    val masterCluster = cluster.map( index => {
      val start = """[""""
      val end = """"]"""
      val cent = centroid(index.toInt).zipWithIndex.map( row => (row._2, row._1)).filter(_._2 > 0.0).map(_.toString).mkString(start, "\",\"", end)
      val r = radius(index.toInt)
      val i = index.toInt
      val size = n(index.toInt)
      Document.parse(s"{cluster: $i, radius: $r, n: $size, centroid : $cent}")
    })
    masterCluster
  }

  val onlineClustering = udf((content: Seq[String]) => {

    println("=========================== Online Clustering ===============================")
    // masterWord.foreach(println)

    size = AggTools.masterWord.size

    // convert New Data to Array
    var tempSeq = content.map( row => {
      var word = row.drop(1).dropRight(1).split("\\,")
      (word(0).toInt, word(1).toDouble)
    }).toSeq

    val newData = Vectors.sparse(size, tempSeq.sortWith(_._1 < _._1)).toDense.toArray

    val distData = centroidArr.map(data => {
      var cent = data._1.map( row => {
        var word = row.drop(1).dropRight(1).split("\\,")
        (word(0).toInt, word(1).toDouble)
      }).toSeq
      // var cent = centTupple.drop(1).dropRight(1).split("\\,")
      var centVec = Vectors.sparse(size, cent.sortWith(_._1 < _._1)).toDense.toArray
      var dist = 1 - CosineSimilarity.cosineSimilarity(centVec, newData)
      println((data._2, dist, data._4 ))

      if(dist > data._4){
        dist = 1
      }

      (data._2, dist)
    })

    var selected = distData.sortWith(_._2 < _._2)(0)
    var clusterSelected = selected._1
    if(selected._2 == 1) {
      clusterSelected = 0
    }
    clusterSelected

    // val vectorData = tempSeq.sortWith(_._1 < _._1)
    // vectorData.map(_.toString)
    // vectorData
  })
}
