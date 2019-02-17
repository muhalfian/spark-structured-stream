package com.muhalfian.spark.util

import org.apache.spark.sql.types._

object ColsArtifact {
  val rawSchema : StructType = StructType(Seq(
    StructField("link", StringType,true),
    StructField("source", StringType, true),
    StructField("description", StringType, true),
    StructField("image", StringType, true),
    StructField("publish_date", StringType, true),
    StructField("title", StringType, true),
    StructField("text", StringType, true)
    )
  )

  val preprocessSchema : StructType = StructType(Seq(
    StructField("link", StringType,true),
    StructField("source", StringType, true),
    StructField("description", StringType, true),
    StructField("image", StringType, true),
    StructField("publish_date", StringType, true),
    StructField("title", StringType, true),
    StructField("text", StringType, true),
    StructField("text_preprocess", ArrayType(StringType), true),
    StructField("text_selected", ArrayType(StringType), true)
    )
  )

  case class masterData(
    link: String,
    source: String,
    description: String,
    image: String,
    publish_date: String,
    title: String,
    text: String,
    text_preprocess: String,
    text_selected: String,
    text_aggregate: String
  )
}
