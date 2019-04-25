package com.muhalfian.spark.util

import org.apache.spark.sql.Row
import scala.collection.mutable.WrappedArray
import scala.collection.mutable.ArrayBuffer
import collection.JavaConverters._

object RowArtifact {

  def rowMasterData(r:Row) = {
    ColsArtifact.masterData(
      r.getAs[String](0),
      r.getAs[String](1),
      r.getAs[String](2),
      r.getAs[String](3),
      r.getAs[String](4),
      r.getAs[String](5),
      r.getAs[String](6),
      r.getAs[String](7),
      r.getAs[String](8),
      r.getAs[String](9),
      r.getAs[String](10)
    )
  }

  def rowMasterDataUpdate(r:Row) = {
    ColsArtifact.masterDataUpdate(
      r.getAs[String](0),
      r.getAs[String](1),
      r.getAs[String](2),
      r.getAs[String](3),
      r.getAs[String](4),
      r.getAs[String](5),
      r.getAs[String](6),
      r.getAs[String](7),
      ArrayBuffer(r.getAs[WrappedArray[String]](8).toSeq : _*),
      ArrayBuffer(r.getAs[WrappedArray[String]](9).toSeq.toArray : _*),
      ArrayBuffer(r.getAs[WrappedArray[String]](10).toSeq.toArray : _*),
      r.getAs[String](11),
      r.getAs[Double](12)
    )
  }

  def clusterData(r:Row) = {
    ColsArtifact.masterData(
      r.getAs[String](0),
      r.getAs[String](1),
      r.getAs[String](2),
      r.getAs[String](3),
      r.getAs[String](4),
      r.getAs[String](5),
      r.getAs[String](6),
      r.getAs[String](7),
      r.getAs[String](8),
      r.getAs[String](9),
      r.getAs[String](10)
    )
  }
}
