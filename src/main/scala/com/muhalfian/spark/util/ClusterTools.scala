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
import java.util.Calendar


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
  val writeConfig = WriteConfig(Map("uri" -> "mongodb://10.252.37.112/prayuga", "database" -> "prayuga", "collection" -> "master_cluster_10"))
  val readConfig = ReadConfig(Map("uri" -> "mongodb://10.252.37.112/prayuga", "database" -> "prayuga", "collection" -> "master_cluster_4"))

  val spark = OnlineStream.spark
  val sc = spark.sparkContext
  import spark.implicits._

  // read master cluster
  // var centroidArr = MongoSpark.load(spark, readConfig).collect
  // centroidArr.foreach(println)
  var centroids = MongoSpark.load(spark, readConfig)
  .map(row => {
    (row.getAs[Seq[String]]("centroid"),row.getAs[Integer]("cluster"),row.getAs[Integer]("n"),row.getAs[Double]("radius"))
  }).collect
  var centroidArr = ArrayBuffer(centroids: _*)
  centroidArr.foreach(println)

  // calculate unknown cluster
  var size = AggTools.masterWord.size
  var unknown = centroidArr.map(data => {
    var cent = data._1.map( row => {
      var word = row.drop(1).dropRight(1).split("\\,")
      (word(0).toInt, word(1).toDouble)
    }).toSeq
    // var cent = centTupple.drop(1).dropRight(1).split("\\,")
    var centVec = Vectors.sparse(size, cent.sortWith(_._1 < _._1)).toDense.toArray
    var zeroVec = Array.fill(size)(0.01)
    var dist = 1 - CosineSimilarity.cosineSimilarity(centVec, zeroVec)
    (data._2, dist)
  }).minBy(_._2)._1
  println("unknown cluster : " + unknown)

  // calculate rmax
  var dmax = centroidArr.filter(x => x._2 != unknown).maxBy(_._4)._4
  println("dmax = " + dmax)

  // cluster definition
  var alpha = 0.1



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

  val updateRadius = udf((content: Seq[String], index: Integer ) => {
    size = AggTools.masterWord.size

    // convert New Data to Array
    var tempSeq = content.map( row => {
      var word = row.drop(1).dropRight(1).split("\\,")
      (word(0).toInt, word(1).toDouble)
    }).toSeq
    val dataVec = Vectors.sparse(size, tempSeq.sortWith(_._1 < _._1)).toDense.toArray

    // loop centroid data then calculate distance
    val centroid = centroidArr.filter(_._2 == index)(0)._1.map( row => {
        var word = row.drop(1).dropRight(1).split("\\,")
        (word(0).toInt, word(1).toDouble)
      }).toSeq
    var centVec = Vectors.sparse(size, centroid.sortWith(_._1 < _._1)).toDense.toArray

    // calculate distance
    var radius = 1 - CosineSimilarity.cosineSimilarity(centVec, dataVec)

    radius
  })

  val onlineClustering = udf((content: Seq[String]) => {

    println("=========================== Online Clustering ===============================")
    // masterWord.foreach(println)

    // update size array [word]
    size = AggTools.masterWord.size

    // convert New Data to Array
    var tempSeq = content.map( row => {
      var word = row.drop(1).dropRight(1).split("\\,")
      (word(0).toInt, word(1).toDouble)
    }).toSeq
    val newData = Vectors.sparse(size, tempSeq.sortWith(_._1 < _._1)).toDense.toArray

    // loop centroid data then calculate distance
    val distData = centroidArr.map(data => {
      var cent = data._1.map( row => {
        var word = row.drop(1).dropRight(1).split("\\,")
        (word(0).toInt, word(1).toDouble)
      }).toSeq
      var centVec = Vectors.sparse(size, cent.sortWith(_._1 < _._1)).toDense.toArray

      // calculate distance
      var dd = 1 - CosineSimilarity.cosineSimilarity(centVec, newData)
      println((data._2, dd, data._3 ))

      // compare to radius
      var beta = 1
      var dt = beta * dmax
      if(dd > dt){
        dd = 1
      }

      (data._2, dd, data._3, data._4)
    })

    println("=============== CLUSTER DISTANCE LIST ====================")
    distData.foreach(println)

    var clusterSelected = 0
    var selected = distData.minBy(_._2)
    if(selected._2 == 1) {
      // make new cluster
      println("============= NEW CLUSTER =====================")
      clusterSelected = distData.size + 1
      println("cluster selected = " + clusterSelected)
      println("cluster distance = " + selected._2)
      var newSize = 1
      var newCentroid = newData.zipWithIndex.map( row => (row._2, row._1)).filter(_._2 > 0.0).map(_.toString).toList
      var newRadius = 0
      centroidArr += ((newCentroid, clusterSelected, newSize, newRadius))

      // add mongo
      val start = """[""""
      val end = """"]"""
      var newCentroidStr = newCentroid.mkString(start, "\",\"", end)
      val datetime = Calendar.getInstance().getTime()
      var newDoc = sc.parallelize(Seq(Document.parse(s"{cluster : $clusterSelected, radius: $newRadius, n: $newSize, $centroid: newCentroidStr, $datetime: datetime}")))
      MongoSpark.save(newDoc, writeConfig)

    } else {
      println("============= UPDATE CLUSTER =====================")
      clusterSelected = selected._1
      println("cluster selected = " + clusterSelected)
      println("cluster distance = " + selected._2)

      // update centroid
      var centroidSelected = centroidArr.filter(_._2 == clusterSelected)(0)._1.map( row => {
        var word = row.drop(1).dropRight(1).split("\\,")
        (word(0).toInt, word(1).toDouble)
      }).toSeq
      var centroidSelectedArr = Vectors.sparse(size, centroidSelected.sortWith(_._1 < _._1)).toDense.toArray
      var newCentroidArr = Array.ofDim[Double](centroidSelectedArr.size)
      for ( i <- 0 to (centroidSelectedArr.length - 1) ) {
        newCentroidArr(i) = centroidSelectedArr(i) + (alpha * (newData(i) - centroidSelectedArr(i)))
      }
      var updateCentroid = newCentroidArr.zipWithIndex.map( row => (row._2, row._1)).filter(_._2 > 0.0).map(_.toString).toList

      var updateSize = selected._3 + 1
      var updateRadius = selected._4

      // update centroid
      var index = centroidArr.indexWhere(_._2 == clusterSelected)
      centroidArr(index) = (updateCentroid, clusterSelected, updateSize, updateRadius)

      // update - add to mongo
      val datetime = Calendar.getInstance().getTime()
      var newDoc = sc.parallelize(Seq(Document.parse(s"{cluster : $clusterSelected, radius: $updateRadius, n: $updateSize, $centroid: updateCentroid, $datetime: datetime}")))
      MongoSpark.save(newDoc, writeConfig)
    }
    centroidArr.foreach(println)
    clusterSelected

    // val vectorData = tempSeq.sortWith(_._1 < _._1)
    // vectorData.map(_.toString)
    // vectorData
  })
}
