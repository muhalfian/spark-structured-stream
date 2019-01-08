package com.muhalfian.spark.util

import org.apache.spark.sql.Row

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
      r.getAs[String](9)
    )
  }

}
