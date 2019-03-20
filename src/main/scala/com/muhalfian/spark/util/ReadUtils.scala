package com.muhalfian.spark.util

import com.muhalfian.spark.jobs.OnlineStream

// import org.bson.Document
import org.apache.spark.rdd.RDD
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD

// import org.mongodb.scala.Document

import org.apache.spark.sql._

object ReadUtils {
  val uri = PropertiesLoader.mongoUrl
  val db = PropertiesLoader.mongoDb

  val spark = OnlineStream.spark
  val sc = spark.sparkContext
  import spark.implicits._

  // master cluster
  val collection = "master_cluster_3"
  val masterCluster = MongoSpark.load(spark, ReadConfig(Map("uri" -> uri, "database" -> db, "collection" -> collection)))

  def getMasterCluster() = {
    masterCluster.map(row => {
      (row.getAs[Seq[String]]("centroid"),row.getAs[Integer]("cluster"),row.getAs[Integer]("n"),row.getAs[Double]("radius"))
    }).collect
  }

  def getMasterCluster() = {
    val centroids = masterCluster.map(row => {
      (row.getAs[Seq[String]]("centroid"),row.getAs[Integer]("cluster"),row.getAs[Integer]("n"),row.getAs[Double]("radius"))
    }).collect

    ArrayBuffer(centroids: _*)
  }

}
