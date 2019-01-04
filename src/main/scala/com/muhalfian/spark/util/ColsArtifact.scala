package com.muhalfian.spark.util

import org.apache.spark.sql.types._

object ColsArtifact {
  val rawSchema : StructType = StructType(Seq(
    StructField("link", StringType,true),
    StructField("source", StringType, true),
    StructField("authors", StringType, true),
    StructField("image", StringType, true),
    StructField("publish_date", StringType, true),
    StructField("title", StringType, true),
    StructField("text", StringType, true)
    )
  )

  case class ConnCountObj(
    link: String,
    source: String,
    authors: String,
    image: String,
    publish_date: String,
    title: String,
    text: String,
    text_preprocess: String,
    text_aggregation: String
  )
}
