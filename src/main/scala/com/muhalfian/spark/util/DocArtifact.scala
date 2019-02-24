package com.muhalfian.spark.util

import org.bson.Document

object DocArtifact {

  def dataInit(r:Document) = {
    ColsArtifact.dataInit(
      r.getAs[String](0),
      r.getAs[String](1),
      r.getAs[String](2),
      r.getAs[String](3),
      r.getAs[String](4),
      r.getAs[String](5),
      r.getAs[String](6),
      r.getAs[Array](7),
      r.getAs[Array](8)
    )
  }
}
