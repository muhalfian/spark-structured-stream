package com.muhalfian.spark.util

import com.muhalfian.spark.jobs.OnlineStream
import com.muhalfian.spark.models._

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
import java.util.UUID.randomUUID


import collection.JavaConverters._


object ClusterTools {

  // initialization ALI lib
  val clib : ClusteringLib = new ClusteringLib();
  val vlib: VectorLib = new VectorLib()

  // Initialization Cluster Tools
  var clusterArray = Array.ofDim[String](1)
  var centroid = Array[Array[Double]](Array(1.0))
  var distance = Array.ofDim[Double](1)
  var radius = Array.ofDim[Double](1)
  var n = Array.ofDim[Int](1)
  var size = MasterWordModel.masterWordArr.size
  val mongoDb = PropertiesLoader.mongoUrl + PropertiesLoader.mongoDb

  // MongoConfig
  val writeConfig = WriteConfig(Map("uri" -> mongoDb, "collection" -> PropertiesLoader.dbMasterClusterUpdate))

  val spark = OnlineStream.spark
  val sc = spark.sparkContext
  import spark.implicits._

  // read master cluster
  // var centroidArr = ArrayBuffer[(Seq[String], Integer, Integer, Double)]((Seq("test"),0,0,0.0))
  // var dmax = 0.0
  var centroidArr = MasterClusterModel.masterClusterArr
  var dmax = MasterClusterModel.getDmax()

  def getCentroid(aggregateArray: Array[Array[Double]] , clusterArray: Array[String] ) = {
    // merge cluster, array
    var dataArray = aggregateArray.zipWithIndex.map(data => {
      (clusterArray(data._2), data._1)
    }).groupBy(_._1)

    // find centroid
    for ((key, value) <- dataArray) {
      val data = value.map(arr => arr._2)
      centroid(key.toInt) = clib.getCentroid(data)
    }
  }

  def calculateDistance(aggregateArray: Array[Array[Double]] , clusterArray: Array[String]) = {
    for ( i <- 1 to (aggregateArray.length - 1) ) {
      val cent = centroid(clusterArray(i).toInt)
      val data = aggregateArray(i)
      distance(i) =  vlib.getDistance(cent, data)
    }
  }

  def calculateRadius(aggregateArray: Array[Array[Double]] , clusterArray: Array[String] ) = {
    // merge cluster, array
    var dataArray = aggregateArray.zipWithIndex.map(data => {
      (clusterArray(data._2), distance(data._2))
    }).groupBy(_._1)

    // find radius
    for ((key, value) <- dataArray) {
      val dist = value.map(arr => arr._2)
      radius(key.toInt) = dist.max
      n(key.toInt) = dist.size
    }
  }

  def masterDataAgg(mongoRDD: RDD[org.bson.Document]) : RDD[org.bson.Document] = {
    val masterData = mongoRDD.zipWithIndex.map( row => {
      // var result = word.replaceAll("[\\]\\[]", "")
      // result = result.replaceAll("\"", " ")
      // row._1.text_preprocess = row._1.text_preprocess.replaceAll("[\\]\\[]", "")
      // println(row._1.toString())
      println(row._1.get("text_preprocess").toString())
      row._1.put("cluster", clusterArray(row._2.toInt))
      row._1.put("to_centroid", distance(row._2.toInt))
      row._1
    })
    masterData
  }

  def masterClusterAgg(mongoRDD : RDD[org.bson.Document], cluster: Array[String]): Array[org.bson.Document] = {
    val masterCluster = cluster.map( index => {
      val i = index.toString
      val r = radius(index.toInt)
      val size = n(index.toInt)
      val cent = centroid(index.toInt).zipWithIndex.map( row => (row._2, row._1)).filter(_._2 > 0.0).map( row => row.toString)
      val centStr = convertSeqToString(cent)
      val to_ground = getDistaceToGround(cent)
      val angle_ground = getCosineToGround(cent)
      val datetime = getTimeStamp()
      val link_id = "predefined"
      Document.parse(s"{cluster: '$i', radius: $r, n: $size, centroid : $centStr, to_ground : $to_ground, angle_ground : $angle_ground, datetime: $datetime, link_id: '$link_id'}")
    })
    masterCluster
  }

  def getCosineToGround(cent: Seq[String]) : Double = {
    var centVec = ClusterTools.convertSeqToFeatures(cent)
    var zeroVec = Array.fill(size)(0.01)
    var dist = CosineSimilarity.cosineSimilarity(centVec, zeroVec)
    dist
  }

  def getDistaceToGround(cent : Seq[String]) : Double = {
    val centVec = ClusterTools.convertSeqToFeatures(cent)
    val size = centVec.size
    val zeroVec = Array.fill(size)(0.0)
    val dist = ClusterTools.vlib.getDistance(centVec, zeroVec)
    dist
  }

  def convertSeqToFeatures(data : Seq[String]) : Array[Double] = {
    val tempSeq = data.map( row => {
      var word = row.drop(1).dropRight(1).split("\\,")
      (word(0).toInt, word(1).toDouble)
    }).toSeq
    val features = Vectors.sparse(size, tempSeq.sortWith(_._1 < _._1)).toDense.toArray
    features
  }

  def convertSeqToString(data: Seq[String]): String = {
    val start = """[""""
    val end = """"]"""
    var dataStr = data.map(_.toString).mkString(start, "\",\"", end)
    dataStr
  }

  def convertFeaturesToSeq(data : Array[Double]) : Seq[String] = {
    val stringData = data.zipWithIndex.map( row => (row._2, row._1)).filter(_._2 > 0.0).map(_.toString).toList
    stringData
  }

  def getDistanceToCentroids(newData : Array[Double]) : ArrayBuffer[(String, Integer, Double, Double, Integer)] = {
    val distance = centroidArr.map(data => {
      val centVec = convertSeqToFeatures(data._1)
      val dd = vlib.getDistance(centVec, newData)

      // compare to radius
      val beta = 1
      val dt = beta * dmax
      val action : Integer = if(dd > dt) 1 else 0

      // cluster, n, radius, distance, action
      (data._2.toString, data._3, data._4, dd, action)
    })

    distance
  }

  def getTimeStamp() : Long = {
    val timestamp = java.lang.System.currentTimeMillis / 1000
    timestamp
  }

  def addCentroidArr(newCentroid: Seq[String], clusterSelected: String, newSize: Integer, newRadius: Double, link: String) = {
    centroidArr += ((newCentroid, clusterSelected, newSize, newRadius, link))
  }

  def addCentroidMongo(newCentroid: Seq[String], clusterSelected: String, newSize: Integer, newRadius: Double, link: String) = {
    val newCentroidStr = convertSeqToString(newCentroid)
    val datetime = getTimeStamp()
    // val to_ground = getDistaceToGround(newCentroid)
    // val angle_ground = getCosineToGround(newCentroid)
    val to_ground = 0
    val angle_ground = 0
    var newDoc = sc.parallelize(Seq(Document.parse(s"{cluster : '$clusterSelected', radius: $newRadius, n: $newSize, centroid: $newCentroidStr, to_ground : $to_ground, angle_ground : $angle_ground, datetime: $datetime, link: '$link'}")))
    MasterClusterModel.save(newDoc)
  }

  def updateCentroidArr(updateCentroid: Seq[String], newCluster: String, updateSize: Integer, updateRadius: Double, link: String) = {
    var index = centroidArr.indexWhere(_._2 == newCluster)
    centroidArr(index) = (updateCentroid, newCluster, updateSize, updateRadius, link)
  }

  def vectorQuantization(centroid: Double, newData: Double): Double = {
    val alpha = 0.1
    val vq = centroid + (alpha * (newData - centroid))
    vq
  }

  def getUpdateCentroid(centroid: Array[Double], newData: Array[Double]): Seq[String] = {
    var newCentroid = Array.ofDim[Double](centroid.size)
    for ( i <- 0 to (centroid.length - 1) ) {
      newCentroid(i) = vectorQuantization(centroid(i), newData(i))
    }
    var newCentroidSeq = convertFeaturesToSeq(newCentroid)
    newCentroidSeq
  }

  def getUpdateRadius(selectedCluster : (String, Integer, Double, Double, Integer)) : Double = {
    val updateRadius = if(selectedCluster._4 > selectedCluster._3) selectedCluster._4 else selectedCluster._3
    updateRadius
  }

  def actionNewCluster(newData: Array[Double], link: String) : String = {
    println("============= NEW CLUSTER =====================")
    var newCluster = centroidArr.size.toString
    // var newCluster = randomUUID().toString
    println("cluster selected = " + newCluster + " [NEW]")
    println("cluster distance = 0")

    var newSize = 1
    var newCentroid = convertFeaturesToSeq(newData)
    var newRadius = 0

    // sink
    addCentroidArr(newCentroid, newCluster, newSize, newRadius, link)
    addCentroidMongo(newCentroid, newCluster, newSize, newRadius, link)

    newCluster
  }

  def actionUpdateCluster(newData: Array[Double], selectedCluster: (String, Integer, Double, Double, Integer), link: String) : String = {
    var newCluster = selectedCluster._1
    println("============= UPDATE CLUSTER =====================")
    println("cluster selected = " + newCluster)
    println("cluster distance = " + selectedCluster._2)

    // update centroid
    var centroid = convertSeqToFeatures(centroidArr.filter(_._2 == newCluster)(0)._1)
    var updateCentroid  = getUpdateCentroid(centroid, newData)

    var updateSize = selectedCluster._2 + 1
    var updateRadius = getUpdateRadius(selectedCluster)

    // sink
    updateCentroidArr(updateCentroid, newCluster, updateSize, updateRadius, link)
    addCentroidMongo(updateCentroid, newCluster, updateSize, updateRadius, link)

    newCluster
  }

  def linkCheck(link: String) = {
    centroidArr.indexWhere(_._5 == link)
  }

  val updateRadius = udf((content: Seq[String], index: String ) => {
    size = MasterWordModel.masterWordArr.size

    val dataVec = convertSeqToFeatures(content)
    val centVec = convertSeqToFeatures(centroidArr.filter(_._2 == index)(0)._1)

    // calculate distance
    var radius = vlib.getDistance(centVec, dataVec)
    radius
  })

  val onlineClustering = udf((content: Seq[String], link: String) => {

    var newCluster = "0"
    var index = linkCheck(link)
    if(index == -1) {
      println("########### Start Online Clustering ##################")
      // update size array [word]
      size = MasterWordModel.masterWordArr.size

      val newData = convertSeqToFeatures(content)
      println("new data")
      println(newData)
      val selectedCluster = getDistanceToCentroids(newData).minBy(_._4)
      println("selected cluster")
      println(selectedCluster)

      println("if condition")

      index = linkCheck(link)
      if(index == -1){
        if(selectedCluster._5 == 1){
          newCluster = actionNewCluster(newData, link)
        } else {
          newCluster = actionUpdateCluster(newData, selectedCluster, link)
        }
      } else {
        println("************* PASSING *************")
        newCluster = centroidArr(index)._2
      }
    } else {
      println("************* PASSING *************")
      newCluster = centroidArr(index)._2
    }

    newCluster
  })
}
